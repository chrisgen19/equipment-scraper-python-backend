from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import logging
from datetime import datetime
from urllib.parse import urljoin
import re

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# -------------------------
# Logging & Flask setup
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Allow localhost frontend

# -------------------------
# Your scraper class (lightly adapted)
# -------------------------
class EquipmentTraderScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.scraped_data = []

    async def scrape_listing_page(self, page, listing_url):
        """Scrape individual listing page for detailed information"""
        try:
            logger.info(f"Scraping listing: {listing_url}")
            await page.goto(listing_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(2)

            # Try multiple selectors for the main content
            selectors_to_try = [
                '.dealer-details-container',
                '[class*="dealer-details"]',
                'h1[id*="dealer-details"]',
                '.addetail-info'
            ]

            for selector in selectors_to_try:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    break
                except Exception:
                    continue

            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')

            listing_data = {}

            # Title & subtitle
            title_container = soup.find('h1', {'id': 'dealer-details-main-h1-version'})
            if not title_container:
                title_container = soup.find('h1', string=re.compile(r'.*NIFTYLIFT.*|.*Boom Lift.*', re.I)) or soup.find('h1')
            if title_container:
                subtitle_elem = title_container.find('span', class_='dealer-details-subtitle')
                title_elem = title_container.find('span', class_='dealer-details-title')
                if subtitle_elem and title_elem:
                    listing_data['subtitle'] = subtitle_elem.get_text(strip=True)
                    listing_data['title'] = title_elem.get_text(strip=True)
                else:
                    full_title = title_container.get_text(strip=True)
                    listing_data['subtitle'] = full_title
                    listing_data['title'] = ""

            # Price
            price_text = ""
            for sel in ['span#addetail-price-detail', '.addetail-price', '[class*="price"]', '[id*="price"]']:
                el = soup.select_one(sel)
                if el and ('$' in el.get_text() or re.search(r'\d', el.get_text())):
                    price_text = el.get_text(strip=True)
                    break

            # Dealer name (best-effort)
            dealer_name = ""
            for sel in ['.dealer-name', '[class*="dealer"]', '[class*="contact"]']:
                el = soup.select_one(sel)
                if el:
                    dealer_name = re.sub(r'^Contact\s*', '', el.get_text(strip=True), flags=re.I)
                    if dealer_name:
                        break

            # Phone
            phone = ""
            page_text = soup.get_text(" ", strip=True)
            for pattern in [r'1-\d{3}-\d{3}-\d{4}', r'\d{3}-\d{3}-\d{4}', r'\(\d{3}\)\s*\d{3}-\d{4}']:
                m = re.search(pattern, page_text)
                if m:
                    phone = m.group(0)
                    break

            # Listing code
            listing_code = ""
            m = re.search(r'Listing Code:\s*([A-Za-z0-9-]+)', page_text, re.I)
            if m:
                listing_code = m.group(1)

            # Specs (structured list first)
            specs = {}
            info_list = soup.find('ul', class_='info-list-seller') or soup.find('ul', class_='list-unstyled')
            if info_list:
                for li in info_list.find_all('li'):
                    h3 = li.find('h3')
                    if not h3:
                        continue
                    left = h3.find('span', class_='detailsListItemLeft')
                    right = h3.find('span', class_='detailsListItemRight')
                    if left and right:
                        key = left.get_text(strip=True).replace(':', '').lower()
                        value = right.get_text(strip=True)
                        specs[key] = value
                    else:
                        # fallback parse
                        h3_text = h3.get_text(" ", strip=True)
                        if ':' in h3_text:
                            k, v = h3_text.split(':', 1)
                            specs[k.strip().lower()] = v.strip()
            else:
                # Fallback regexes
                spec_patterns = {
                    'condition': r'Condition:\s*([^\n\r]+)',
                    'year': r'Year:\s*(\d{4})',
                    'make': r'Make:\s*([^\n\r]+)',
                    'model': r'Model:\s*([^\n\r]+)',
                    'category': r'Category:\s*([^\n\r]+)',
                    'location': r'Location:\s*([^\n\r]+)'
                }
                for k, pat in spec_patterns.items():
                    m2 = re.search(pat, page_text, re.I)
                    if m2:
                        specs[k] = m2.group(1).strip()

            # Dealer website
            dealer_website = ""
            website_elem = soup.find('a', {'id': 'itemWebsite'}) or soup.find('a', href=re.compile(r'http.*dealer.*website', re.I))
            if website_elem and website_elem.get('href'):
                dealer_website = website_elem['href']

            # Build unified record (map to frontend schema)
            make = specs.get('make', '')
            model = specs.get('model', '')
            condition = specs.get('condition', '')
            location = specs.get('location', '')

            # normalize price (keep original string)
            price = price_text.strip()

            result = {
                # front-end expected keys:
                'brand': make,
                'model': model,
                'condition': condition,
                'location': location,
                'price': price,
                'url': listing_url,

                # extra fields (ignored by your table, but might be useful)
                'title': listing_data.get('title', ''),
                'subtitle': listing_data.get('subtitle', ''),
                'year': specs.get('year', ''),
                'category': specs.get('category', ''),
                'dealer_name': dealer_name,
                'phone': phone,
                'listing_code': listing_code,
                'dealer_website': dealer_website,
                'scraped_at': datetime.now().isoformat()
            }

            logger.info(f"OK: {result['brand']} {result['model']} - {result['price']}")
            return result

        except Exception as e:
            logger.error(f"Error scraping listing {listing_url}: {e}")
            return None

    async def scrape_search_results(self, page):
        """Scrape the search results page to get listing URLs"""
        listing_urls = []
        try:
            await page.goto(self.base_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)

            selectors_to_try = [
                'article[data-ad-id]',
                '[data-ad-id]',
                'article[class*="search-card"]',
                '.search-card',
                '[class*="listing"]',
                'a[href*="/listing/"]'
            ]

            handles = []
            for sel in selectors_to_try:
                try:
                    await page.wait_for_selector(sel, timeout=3000)
                    handles = await page.query_selector_all(sel)
                    if handles:
                        logger.info(f"Found {len(handles)} elements via '{sel}'")
                        break
                except Exception:
                    continue

            if not handles:
                # fallback parse via HTML
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                for a in soup.find_all('a', href=re.compile(r'/listing/')):
                    href = a.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        listing_urls.append(full_url)
                listing_urls = list(dict.fromkeys(listing_urls))
                logger.info(f"Fallback found {len(listing_urls)} URLs")
                return listing_urls

            # extract from found nodes
            for node in handles:
                try:
                    link = await node.query_selector('a[href*="/listing/"]')
                    href = None
                    if link:
                        href = await link.get_attribute('href')
                    if not href:
                        # maybe the node itself is <a>
                        href = await node.get_attribute('href')
                    if href and '/listing/' in href:
                        listing_urls.append(urljoin(self.base_url, href))
                except Exception:
                    continue

            listing_urls = list(dict.fromkeys(listing_urls))
            logger.info(f"Collected {len(listing_urls)} unique listing URLs")
            return listing_urls

        except Exception as e:
            logger.error(f"Error scraping search results: {e}")
            # final fallback
            try:
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                fallback = []
                for a in soup.find_all('a', href=re.compile(r'/listing/')):
                    href = a.get('href')
                    if href:
                        fallback.append(urljoin(self.base_url, href))
                fallback = list(dict.fromkeys(fallback))
                logger.info(f"Final fallback found {len(fallback)} URLs")
                return fallback
            except Exception as e2:
                logger.error(f"Fallback failed: {e2}")
                return []

    async def scrape_all_pages(self, max_pages=1):
        """Scrape up to max_pages of results and populate self.scraped_data"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1600, 'height': 900}
            )
            page = await context.new_page()
            await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            try:
                current = 1
                all_urls = []

                while current <= max_pages:
                    logger.info(f"Scraping search page {current}")
                    # NOTE: we always ask the page object to navigate based on base_url
                    # (your scrape_search_results reads self.base_url directly)
                    urls = await self.scrape_search_results(page)
                    if not urls:
                        break
                    all_urls.extend(urls)
                    current += 1

                all_urls = list(dict.fromkeys(all_urls))
                logger.info(f"Total listing URLs: {len(all_urls)}")

                for i, u in enumerate(all_urls, 1):
                    logger.info(f"Listing {i}/{len(all_urls)}")
                    item = await self.scrape_listing_page(page, u)
                    if item:
                        self.scraped_data.append(item)
                    await asyncio.sleep(1.5)

            finally:
                await browser.close()

# -------------------------
# API endpoints
# -------------------------
@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'OK',
        'message': 'Backend is running',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    """POST body: { "url": "<equipmenttrader search url>", "max_pages": 1 }"""
    data = request.get_json(silent=True) or {}
    url = data.get('url')
    max_pages = int(data.get('max_pages') or 1)

    if not url or 'equipmenttrader.com' not in url:
        return jsonify({'success': False, 'error': 'Please provide a valid equipmenttrader.com URL'}), 400

    scraper = EquipmentTraderScraper(url)

    start = datetime.now()
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(scraper.scrape_all_pages(max_pages=max_pages))
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        loop.close()

    duration = (datetime.now() - start).total_seconds()

    # Return only the fields your React table expects + keep originals intact
    payload = []
    for row in scraper.scraped_data:
        payload.append({
            'brand': row.get('brand', ''),
            'model': row.get('model', ''),
            'condition': row.get('condition', ''),
            'location': row.get('location', ''),
            'price': row.get('price', ''),
            'url': row.get('url', ''),
            # keep extras in case you later want to use them:
            'raw': row
        })

    return jsonify({
        'success': True,
        'data': payload,
        'count': len(payload),
        'duration': f"{duration:.2f}s",
        'timestamp': datetime.now().isoformat()
    })

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Equipment Trader Scraper API',
        'endpoints': {
            'GET /api/health': 'Health check',
            'POST /api/scrape': 'Start scraping (JSON: {url, max_pages?})'
        }
    })

# -------------------------
# Main
# -------------------------
if __name__ == '__main__':
    print("\n==============================================")
    print("🚀 Equipment Trader Scraper API (Flask + Playwright)")
    print("   • GET  /api/health")
    print("   • POST /api/scrape  (body: { url, max_pages? })")
    print("==============================================\n")
    app.run(host='0.0.0.0', port=5001, debug=True)
