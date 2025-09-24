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

    async def scrape_search_results(self, page):
        """
        Scrape the search results page to get all listing URLs, but only
        from within the main results container.
        """
        try:
            logger.info(f"Navigating to search page: {self.base_url}")
            self.emit_progress('phase', {
                'phase': 'loading_search_page',
                'message': 'Loading search results page...'
            })
            
            await page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)
            
            # 1. Define the selector for the main results container
            results_container_selector = 'div.results'
            await page.wait_for_selector(results_container_selector, timeout=10000)
            logger.info("Results container found.")

            self.emit_progress('phase', {
                'phase': 'finding_listings',
                'message': 'Finding listing URLs...'
            })

            # 2. Get a handle for the results container element
            results_container = await page.query_selector(results_container_selector)

            if not results_container:
                logger.warning("Could not find the results container. Exiting.")
                return []

            await page.wait_for_timeout(3000) # Wait for content to load inside

            # 3. Run the query on the container, not the whole page
            #    This selector is more specific to match your request.
            listing_link_selector = 'article.search-card.list[data-ad-id] > a'
            links = await results_container.query_selector_all(listing_link_selector)
            
            if not links:
                logger.warning("No listing links found within the results container. Check for site updates.")
                return []

            listing_urls = []
            for link in links:
                href = await link.get_attribute('href')
                if href and '/listing/' in href:
                    full_url = urljoin(self.base_url, href)
                    listing_urls.append(full_url)
            
            unique_urls = list(dict.fromkeys(listing_urls))
            logger.info(f"Collected {len(unique_urls)} unique listing URLs")
            
            self.emit_progress('urls_found', {
                'total_urls': len(unique_urls),
                'message': f'Found {len(unique_urls)} listings to scrape'
            })
            
            return unique_urls

        except Exception as e:
            logger.error(f"Error scraping search results page {self.base_url}: {e}")
            self.emit_progress('error', {
                'phase': 'search_results',
                'error': str(e)
            })
            return []

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
                    urls_to_scrape = await self.scrape_search_results(page)

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
    max_pages = int(data.get('max_pages') or 1)
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
        'message': 'Scraping started. Connect to progress stream for updates.',
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