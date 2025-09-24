from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import asyncio
import logging
import json
import time
from datetime import datetime
from urllib.parse import urljoin
import re
import threading
from queue import Queue

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# -------------------------
# Logging & Flask setup
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Global progress tracking
progress_queues = {}
active_scraping_sessions = set()

# -------------------------
# Progress Event Manager
# -------------------------
class ProgressManager:
    def __init__(self, session_id):
        self.session_id = session_id
        self.queue = Queue()
        progress_queues[session_id] = self.queue
        
    def emit_progress(self, event_type, data):
        """Emit a progress event"""
        event = {
            'type': event_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        try:
            self.queue.put(event, timeout=1)
        except:
            pass  # Queue full, skip this event
            
    def cleanup(self):
        """Clean up the progress queue"""
        if self.session_id in progress_queues:
            del progress_queues[self.session_id]
        active_scraping_sessions.discard(self.session_id)

# -------------------------
# The Enhanced Scraper Class
# -------------------------
class EquipmentTraderScraper:
    def __init__(self, base_url, session_id=None):
        self.base_url = base_url
        self.scraped_data = []
        self.session_id = session_id
        self.progress_manager = ProgressManager(session_id) if session_id else None

    def emit_progress(self, event_type, data):
        """Helper to emit progress events"""
        if self.progress_manager:
            self.progress_manager.emit_progress(event_type, data)

    async def handle_popups_and_overlays(self, page):
        """
        Handle common popups and overlays that might interfere with scraping
        """
        try:
            # Wait a moment for any popups to appear
            await page.wait_for_timeout(2000)
            
            # Common popup/overlay selectors to dismiss
            popup_selectors = [
                # Cookie consent banners
                'button[id*="cookie"]',
                'button[class*="cookie"]',
                '[data-testid*="cookie"] button',
                'button:has-text("Accept")',
                'button:has-text("Accept Cookies")',
                'button:has-text("I Accept")',
                
                # Modal close buttons
                'button[aria-label="Close"]',
                'button[class*="close"]',
                '[class*="modal"] button[class*="close"]',
                '.modal-close',
                
                # Newsletter/email signup popups
                'button:has-text("No Thanks")',
                'button:has-text("Skip")',
                'button:has-text("Maybe Later")',
                '[class*="newsletter"] button[class*="close"]',
                
                # General dismiss buttons
                'button:has-text("Dismiss")',
                'button:has-text("×")',
                '[role="dialog"] button',
            ]
            
            for selector in popup_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        if await element.is_visible():
                            await element.click(timeout=2000)
                            logger.info(f"Dismissed popup/overlay using selector: {selector}")
                            await page.wait_for_timeout(500)  # Brief wait after dismissal
                            break
                except:
                    continue  # Ignore errors for individual selectors
                    
            # Additional check for any remaining overlays by pressing Escape
            await page.keyboard.press('Escape')
            await page.wait_for_timeout(500)
            
        except Exception as e:
            logger.info(f"Popup handling completed with minor issues: {e}")
            # Don't let popup handling failures stop the main scraping

    async def scrape_listing_page(self, page, listing_url, current_index, total_count):
        """
        Scrape individual listing page for detailed information using specific IDs.
        """
        try:
            logger.info(f"Scraping listing {current_index}/{total_count}: {listing_url}")
            
            # Emit progress for current item
            self.emit_progress('scraping_item', {
                'current_index': current_index,
                'total_count': total_count,
                'url': listing_url,
                'status': 'loading'
            })
            
            await page.goto(listing_url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_timeout(3000) 

            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            def get_spec_by_id(element_id):
                element = soup.find('span', id=element_id)
                return element.get_text(strip=True) if element else ''

            price_element = soup.find('span', id='addetail-price-detail')
            price = price_element.get_text(strip=True) if price_element else 'N/A'

            result = {
                'brand': get_spec_by_id('lct Make'),
                'model': get_spec_by_id('lct Model'),
                'condition': get_spec_by_id('lct Condition'),
                'location': get_spec_by_id('lct Location'),
                'price': price,
                'url': listing_url,
                'raw': {
                    'year': get_spec_by_id('lct Year'),
                    'category': get_spec_by_id('lct Category'),
                    'scraped_at': datetime.now().isoformat()
                }
            }

            if not result['brand'] and not result['model']:
                logger.warning(f"Could not find key details for {listing_url}. Skipping.")
                self.emit_progress('scraping_item', {
                    'current_index': current_index,
                    'total_count': total_count,
                    'url': listing_url,
                    'status': 'skipped',
                    'reason': 'No key details found'
                })
                return None

            logger.info(f"OK: {result['brand']} {result['model']} - {result['price']}")
            
            # Emit successful scrape
            self.emit_progress('scraping_item', {
                'current_index': current_index,
                'total_count': total_count,
                'url': listing_url,
                'status': 'completed',
                'item': {
                    'brand': result['brand'],
                    'model': result['model'],
                    'price': result['price']
                }
            })
            
            return result

        except Exception as e:
            logger.error(f"Error scraping listing {listing_url}: {e}")
            self.emit_progress('scraping_item', {
                'current_index': current_index,
                'total_count': total_count,
                'url': listing_url,
                'status': 'error',
                'error': str(e)
            })
            return None

    async def scrape_search_results(self, page, max_pages=10):
        """
        Scrape the search results page to get all listing URLs from all pages,
        using URL-based pagination. Handles URLs that already have page parameters.
        """
        all_listing_urls = []
        
        # Parse the base URL to detect existing page parameter
        base_url, starting_page = self.parse_url_for_pagination()
        current_page = starting_page
        end_page = starting_page + max_pages - 1
        
        try:
            self.emit_progress('phase', {
                'phase': 'loading_search_page',
                'message': f'Starting pagination from page {starting_page} to {end_page}...'
            })

            while current_page <= end_page:
                try:
                    # Build URL for current page
                    page_url = self.build_page_url(base_url, current_page)
                    
                    logger.info(f"Navigating to page {current_page}: {page_url}")
                    self.emit_progress('pagination', {
                        'current_page': current_page,
                        'max_pages': end_page,
                        'starting_page': starting_page,
                        'message': f'Loading page {current_page}...',
                        'url': page_url
                    })
                    
                    # Navigate to the page URL
                    await page.goto(page_url, wait_until='domcontentloaded', timeout=60000)
                    
                    # Handle any popups that might have appeared
                    await self.handle_popups_and_overlays(page)

                    # Wait for content to load
                    await page.wait_for_timeout(3000)
                    
                    # Check if the results container exists
                    results_container_selector = 'div.results'
                    results_container = await page.query_selector(results_container_selector)
                    
                    if not results_container:
                        logger.warning(f"No results container found on page {current_page}. Likely reached end of results.")
                        self.emit_progress('pagination', {
                            'current_page': current_page,
                            'message': f'No results found on page {current_page}. Reached end of available pages.'
                        })
                        break

                    # Find all listing links on current page
                    listing_link_selector = 'article.search-card.list[data-ad-id] > a'
                    links = await results_container.query_selector_all(listing_link_selector)
                    
                    if not links:
                        logger.info(f"No listing links found on page {current_page}. This appears to be the last page.")
                        self.emit_progress('pagination', {
                            'current_page': current_page,
                            'message': f'No listings found on page {current_page}. Reached end of results.'
                        })
                        break

                    # Extract URLs from this page
                    page_urls = []
                    for link in links:
                        href = await link.get_attribute('href')
                        if href and '/listing/' in href:
                            full_url = urljoin(self.base_url, href)
                            page_urls.append(full_url)
                    
                    if not page_urls:
                        logger.info(f"No valid listing URLs found on page {current_page}")
                        break
                    
                    logger.info(f"Found {len(page_urls)} listings on page {current_page}")
                    all_listing_urls.extend(page_urls)
                    
                    # Emit progress for this page
                    self.emit_progress('page_scraped', {
                        'current_page': current_page,
                        'listings_on_page': len(page_urls),
                        'total_listings_so_far': len(all_listing_urls),
                        'message': f'Page {current_page}: Found {len(page_urls)} listings'
                    })

                    # Check if this page has fewer results than expected (indicating last page)
                    if current_page > starting_page and len(page_urls) < 20:
                        logger.info(f"Page {current_page} has fewer results ({len(page_urls)}), likely the last page")
                        self.emit_progress('pagination', {
                            'current_page': current_page,
                            'message': f'Page {current_page} appears to be the last page (fewer results found)'
                        })
                        current_page += 1  # Include this page in the count
                        break
                    
                    current_page += 1
                    
                    # Small delay between page requests to be respectful
                    await page.wait_for_timeout(1000)
                    
                except Exception as page_error:
                    logger.error(f"Error scraping page {current_page}: {page_error}")
                    
                    # If it's a navigation error, we might have reached the end
                    if "net::ERR_" in str(page_error) or "404" in str(page_error):
                        logger.info(f"Navigation error on page {current_page}, assuming end of results")
                        self.emit_progress('pagination', {
                            'current_page': current_page,
                            'message': f'Reached end of available pages at page {current_page}'
                        })
                        break
                    
                    self.emit_progress('error', {
                        'phase': 'pagination',
                        'page': current_page,
                        'error': str(page_error),
                        'message': f'Error on page {current_page}, stopping pagination'
                    })
                    break
            
            # Remove duplicates while preserving order
            unique_urls = list(dict.fromkeys(all_listing_urls))
            pages_scraped = current_page - starting_page
            
            logger.info(f"Collected {len(unique_urls)} unique listing URLs across {pages_scraped} pages (from page {starting_page} to {current_page-1})")
            
            self.emit_progress('urls_found', {
                'total_urls': len(unique_urls),
                'pages_scraped': pages_scraped,
                'starting_page': starting_page,
                'ending_page': current_page - 1,
                'message': f'Found {len(unique_urls)} unique listings across {pages_scraped} pages (pages {starting_page}-{current_page-1})'
            })
            
            return unique_urls

        except Exception as e:
            logger.error(f"Error in pagination scraping: {e}")
            self.emit_progress('error', {
                'phase': 'search_results',
                'error': str(e)
            })
            return all_listing_urls  # Return whatever we managed to collect

    def parse_url_for_pagination(self):
        """
        Parse the base URL to detect existing page parameter and extract clean base URL.
        Returns (clean_base_url, starting_page_number)
        """
        import re
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        
        try:
            # Parse the URL
            parsed = urlparse(self.base_url)
            query_params = parse_qs(parsed.query)
            
            # Check if page parameter exists
            if 'page' in query_params:
                # Extract the page number
                starting_page = int(query_params['page'][0])
                
                # Remove the page parameter from the URL to get clean base
                clean_params = {k: v for k, v in query_params.items() if k != 'page'}
                clean_query = urlencode(clean_params, doseq=True)
                
                clean_parsed = parsed._replace(query=clean_query)
                clean_base_url = urlunparse(clean_parsed)
                
                logger.info(f"Detected existing page parameter: starting from page {starting_page}")
                logger.info(f"Clean base URL: {clean_base_url}")
                
                return clean_base_url, starting_page
            else:
                # No page parameter, start from page 1
                return self.base_url, 1
                
        except Exception as e:
            logger.warning(f"Error parsing URL for pagination: {e}. Using defaults.")
            return self.base_url, 1

    def build_page_url(self, base_url, page_number):
        """
        Build URL for specific page number.
        """
        if page_number == 1:
            return base_url
        else:
            # Add page parameter to URL
            separator = "&" if "?" in base_url else "?"
            return f"{base_url}{separator}page={page_number}"

    async def scrape_all_pages(self, max_pages=1):
        """
        Main orchestration method to launch browser and scrape pages.
        """
        try:
            self.emit_progress('phase', {
                'phase': 'initializing',
                'message': 'Launching browser...'
            })
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=False,
                    args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
                )
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080}
                )
                page = await context.new_page()
                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                try:
                    urls_to_scrape = await self.scrape_search_results(page, max_pages)

                    if not urls_to_scrape:
                        logger.warning("No URLs found to scrape. Exiting.")
                        self.emit_progress('error', {
                            'message': 'No listings found to scrape'
                        })
                        return

                    self.emit_progress('phase', {
                        'phase': 'scraping_listings',
                        'message': f'Starting to scrape {len(urls_to_scrape)} listings...'
                    })

                    for i, url in enumerate(urls_to_scrape, 1):
                        logger.info(f"Processing listing {i}/{len(urls_to_scrape)}")
                        item_data = await self.scrape_listing_page(page, url, i, len(urls_to_scrape))
                        if item_data:
                            self.scraped_data.append(item_data)
                            
                        # Emit overall progress
                        progress_percentage = (i / len(urls_to_scrape)) * 100
                        self.emit_progress('overall_progress', {
                            'completed': i,
                            'total': len(urls_to_scrape),
                            'percentage': progress_percentage,
                            'successful': len(self.scraped_data)
                        })
                        
                        await asyncio.sleep(1)

                    self.emit_progress('completed', {
                        'total_scraped': len(self.scraped_data),
                        'total_processed': len(urls_to_scrape),
                        'message': f'Scraping completed! Successfully scraped {len(self.scraped_data)} out of {len(urls_to_scrape)} listings.'
                    })

                finally:
                    await browser.close()
                    
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            self.emit_progress('error', {
                'message': f'Scraping failed: {str(e)}'
            })
            raise
        finally:
            if self.progress_manager:
                self.progress_manager.cleanup()

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

@app.route('/api/scrape-progress/<session_id>')
def scrape_progress(session_id):
    """Server-Sent Events endpoint for real-time progress updates"""
    def generate():
        # Send initial connection confirmation
        yield f"data: {json.dumps({'type': 'connected', 'session_id': session_id})}\n\n"
        
        if session_id not in progress_queues:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Session not found'})}\n\n"
            return
            
        queue = progress_queues[session_id]
        
        try:
            while session_id in active_scraping_sessions or not queue.empty():
                try:
                    # Wait for an event with timeout
                    event = queue.get(timeout=1)
                    yield f"data: {json.dumps(event)}\n\n"
                    
                    # Break if scraping completed or errored
                    if event['type'] in ['completed', 'error']:
                        break
                        
                except:
                    # Timeout occurred, send heartbeat
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}\n\n"
                    continue
                    
        except GeneratorExit:
            logger.info(f"Client disconnected from progress stream: {session_id}")
        finally:
            # Cleanup
            if session_id in progress_queues:
                del progress_queues[session_id]

    return Response(generate(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache',
                           'Connection': 'keep-alive',
                           'Access-Control-Allow-Origin': '*'})

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    data = request.get_json(silent=True) or {}
    url = data.get('url')
    max_pages = int(data.get('max_pages') or 10)  # Default to 10 pages max
    session_id = data.get('session_id', f"session_{int(time.time())}")

    if not url or 'equipmenttrader.com' not in url:
        return jsonify({'success': False, 'error': 'Please provide a valid equipmenttrader.com URL'}), 400

    # Add session to active sessions
    active_scraping_sessions.add(session_id)
    
    scraper = EquipmentTraderScraper(url, session_id)
    start = datetime.now()
    
    def run_scraping():
        try:
            asyncio.run(scraper.scrape_all_pages(max_pages=max_pages))
        except Exception as e:
            logger.error(f"Scrape failed: {e}")
            scraper.emit_progress('error', {'message': str(e)})
        finally:
            active_scraping_sessions.discard(session_id)
    
    # Run scraping in background thread
    scraping_thread = threading.Thread(target=run_scraping)
    scraping_thread.daemon = True
    scraping_thread.start()
    
    # Return immediately with session info
    return jsonify({
        'success': True,
        'session_id': session_id,
        'max_pages': max_pages,
        'message': f'Scraping started for up to {max_pages} pages. Connect to progress stream for updates.',
        'progress_url': f'/api/scrape-progress/{session_id}'
    })

@app.route('/api/scrape-result/<session_id>', methods=['GET'])
def get_scrape_result(session_id):
    """Get the final results of a scraping session"""
    # This is a simple implementation - in production you'd want to store results in a database
    return jsonify({
        'success': False,
        'error': 'Results endpoint not implemented. Use progress stream for real-time data.'
    })

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Equipment Trader Scraper API',
        'endpoints': {
            'GET /api/health': 'Health check',
            'POST /api/scrape': 'Start scraping (JSON: {url, session_id?})',
            'GET /api/scrape-progress/<session_id>': 'SSE progress stream'
        }
    })

# -------------------------
# Main
# -------------------------
if __name__ == '__main__':
    print("\n==============================================")
    print("🚀 Equipment Trader Scraper API (Flask + Playwright + SSE)")
    print("   • GET  /api/health")
    print("   • POST /api/scrape  (body: { url, session_id? })")
    print("   • GET  /api/scrape-progress/<session_id>  (SSE)")
    print("==============================================\n")
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)