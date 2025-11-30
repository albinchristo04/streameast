import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Set
from playwright.async_api import async_playwright, Request, Browser, BrowserContext, Page

# Configuration
BASE_URL = "https://xstreameast.com/categories/nba"
M3U8_FILE = "StreamEast.m3u8"
LOG_FILE = "scraper.log"
MAX_CONCURRENT_SCRAPES = 5
REQUEST_TIMEOUT = 30000
MAX_RETRIES = 3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CATEGORY_LOGOS = {
    "StreamEast - PPV Events": "http://drewlive24.duckdns.org:9000/Logos/PPV.png",
    "StreamEast - Soccer": "http://drewlive24.duckdns.org:9000/Logos/Football2.png",
    "StreamEast - F1": "http://drewlive24.duckdns.org:9000/Logos/F1.png",
    "StreamEast - Boxing": "http://drewlive24.duckdns.org:9000/Logos/Boxing-2.png",
    "StreamEast - MMA": "http://drewlive24.duckdns.org:9000/Logos/MMA.png",
    "StreamEast - WWE": "http://drewlive24.duckdns.org:9000/Logos/WWE.png",
    "StreamEast - Golf": "http://drewlive24.duckdns.org:9000/Logos/Golf.png",
    "StreamEast - Am. Football": "http://drewlive24.duckdns.org:9000/Logos/NFL4.png",
    "StreamEast - Baseball": "http://drewlive24.duckdns.org:9000/Logos/MLB.png",
    "StreamEast - Basketball Hub": "http://drewlive24.duckdns.org:9000/Logos/Basketball5.png",
    "StreamEast - Hockey": "http://drewlive24.duckdns.org:9000/Logos/Hockey.png",
    "StreamEast - WNBA": "http://drewlive24.duckdns.org:9000/Logos/WNBA.png",
}

CATEGORY_TVG_IDS = {
    "StreamEast - PPV Events": "PPV.EVENTS.Dummy.us",
    "StreamEast - Soccer": "Soccer.Dummy.us",
    "StreamEast - F1": "Racing.Dummy.us",
    "StreamEast - Boxing": "Boxing.Dummy.us",
    "StreamEast - MMA": "UFC.Fight.Pass.Dummy.us",
    "StreamEast - WWE": "PPV.EVENTS.Dummy.us",
    "StreamEast - Golf": "Golf.Dummy.us",
    "StreamEast - Am. Football": "NFL.Dummy.us",
    "StreamEast - Baseball": "MLB.Baseball.Dummy.us",
    "StreamEast - Basketball Hub": "Basketball.Dummy.us",
    "StreamEast - Hockey": "NHL.Hockey.Dummy.us",
    "StreamEast - WNBA": "WNBA.dummy.us",
}


def categorize_stream(url: str, title: str = "") -> str:
    """Categorize stream based on URL and title keywords."""
    lowered = (url + " " + title).lower()
    
    categories = [
        ("wnba", "StreamEast - WNBA"),
        (["nba", "basketball"], "StreamEast - Basketball Hub"),
        (["nfl", "football"], "StreamEast - Am. Football"),
        (["mlb", "baseball"], "StreamEast - Baseball"),
        (["ufc", "mma"], "StreamEast - MMA"),
        (["wwe", "wrestling"], "StreamEast - WWE"),
        ("boxing", "StreamEast - Boxing"),
        (["soccer", "futbol"], "StreamEast - Soccer"),
        ("golf", "StreamEast - Golf"),
        (["hockey", "nhl"], "StreamEast - Hockey"),
        (["f1", "nascar", "motorsport"], "StreamEast - F1"),
    ]
    
    for keywords, category in categories:
        if isinstance(keywords, list):
            if any(kw in lowered for kw in keywords):
                return category
        else:
            if keywords in lowered:
                return category
    
    return "StreamEast - PPV Events"


async def safe_goto(page: Page, url: str, tries: int = MAX_RETRIES, timeout: int = REQUEST_TIMEOUT) -> bool:
    """Safely navigate to URL with retries and Cloudflare detection."""
    for attempt in range(tries):
        try:
            logger.info(f"Navigating to {url} (attempt {attempt + 1}/{tries})")
            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            
            # Check for Cloudflare challenge
            html = await page.content()
            if any(x in html.lower() for x in ["cloudflare", "just a moment", "attention required"]):
                logger.warning(f"Cloudflare challenge detected on {url}, waiting...")
                await asyncio.sleep(3 + attempt)
                continue
            
            logger.info(f"Successfully loaded {url}")
            return True
            
        except asyncio.TimeoutError:
            logger.warning(f"Timeout loading {url} (attempt {attempt + 1}/{tries})")
        except Exception as e:
            logger.error(f"Error loading {url}: {e}")
        
        if attempt < tries - 1:
            await asyncio.sleep(2 * (attempt + 1))
    
    logger.error(f"Failed to load {url} after {tries} attempts")
    return False


async def get_event_links(page: Page) -> List[str]:
    """Gather all event links from the main page."""
    logger.info("Gathering event links from main page...")
    
    if not await safe_goto(page, BASE_URL):
        logger.error("Failed to load main page")
        return []
    
    try:
        links = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(h => {
                    const sports = ['nba', 'mlb', 'ufc', 'f1', 'soccer', 'wnba', 
                                   'boxing', 'wwe', 'nfl', 'nhl', 'golf', 'mma'];
                    return sports.some(sport => h.includes('/' + sport));
                });
        }""")
        
        unique_links = list(set(links))
        logger.info(f"Found {len(unique_links)} unique event links")
        return unique_links
        
    except Exception as e:
        logger.error(f"Error extracting links: {e}")
        return []


async def scrape_stream_url(context: BrowserContext, url: str) -> Tuple[str, List[str]]:
    """Scrape M3U8 stream URL from event page."""
    m3u8_links: Set[str] = set()
    event_name = "Unknown Event"
    page = await context.new_page()
    
    def capture_request(request: Request):
        """Capture M3U8 requests."""
        req_url = request.url.lower()
        if ".m3u8" in req_url and "master" not in req_url:
            if request.url not in m3u8_links:
                logger.info(f"🎯 Captured stream: {request.url}")
                m3u8_links.add(request.url)
    
    page.on("request", capture_request)
    
    try:
        if not await safe_goto(page, url):
            return event_name, []
        
        # Wait for page to stabilize
        await asyncio.sleep(1.5)
        
        # Extract event name
        event_name = await page.evaluate("""
            () => {
                const selectors = ['h1', '.event-title', '.title', '.stream-title', 'h2'];
                for (let sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent.trim()) {
                        return el.textContent.trim();
                    }
                }
                const title = document.title.trim();
                return title.split('|')[0].trim() || title;
            }
        """)
        
        logger.info(f"Event: {event_name}")
        
        # Try to trigger video player
        try:
            # Click on potential player areas
            await page.mouse.click(500, 400)
            await asyncio.sleep(0.5)
            
            # Try clicking play button if exists
            await page.evaluate("""
                () => {
                    const playButtons = document.querySelectorAll('button, .play-button, .vjs-big-play-button');
                    playButtons.forEach(btn => btn.click());
                }
            """)
        except Exception as e:
            logger.debug(f"Error triggering player: {e}")
        
        # Wait for stream to load
        for i in range(15):
            if m3u8_links:
                break
            await asyncio.sleep(0.5)
        
        if not m3u8_links:
            logger.warning(f"No stream found for {event_name}")
        
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
    finally:
        await page.close()
    
    return event_name, list(m3u8_links)


async def scrape_batch(context: BrowserContext, links: List[str], start_idx: int) -> List[Tuple[str, str, List[str]]]:
    """Scrape a batch of links concurrently."""
    results = []
    tasks = []
    
    for idx, link in enumerate(links, start=start_idx):
        logger.info(f"[{idx}/{start_idx + len(links) - 1}] Processing: {link}")
        task = scrape_stream_url(context, link)
        tasks.append((idx, link, task))
    
    completed = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)
    
    for (idx, link, _), result in zip(tasks, completed):
        if isinstance(result, Exception):
            logger.error(f"Failed to scrape {link}: {result}")
            continue
        
        name, streams = result
        if streams:
            results.append((link, name, streams))
    
    return results


def write_m3u8_file(results: List[Tuple[str, str, List[str]]]) -> None:
    """Write results to M3U8 file."""
    logger.info(f"Writing {len(results)} streams to {M3U8_FILE}")
    
    try:
        with open(M3U8_FILE, "w", encoding="utf-8") as f:
            f.write(f"# Updated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
            f.write(f"# Total streams: {sum(len(streams) for _, _, streams in results)}\n")
            f.write("#EXTM3U\n\n")
            
            stream_count = 0
            for link, name, streams in results:
                category = categorize_stream(link, name)
                logo = CATEGORY_LOGOS.get(category, "")
                tvg_id = CATEGORY_TVG_IDS.get(category, "")
                
                for stream_url in streams:
                    stream_count += 1
                    f.write(f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-logo="{logo}" group-title="{category}",{name}\n')
                    f.write('#EXTVLCOPT:http-user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0\n')
                    f.write('#EXTVLCOPT:http-origin=https://streamscenter.online\n')
                    f.write('#EXTVLCOPT:http-referrer=https://streamscenter.online/\n')
                    f.write(f'{stream_url}\n\n')
        
        logger.info(f"✅ Successfully wrote {stream_count} streams to {M3U8_FILE}")
        
    except Exception as e:
        logger.error(f"Error writing M3U8 file: {e}")
        raise


async def main():
    """Main execution function."""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("StreamEast Scraper Started")
    logger.info("=" * 80)
    
    try:
        async with async_playwright() as p:
            # Launch browser
            logger.info("Launching browser...")
            browser = await p.firefox.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
                viewport={'width': 1920, 'height': 1080},
                locale='en-US'
            )
            
            # Get event links
            main_page = await context.new_page()
            links = await get_event_links(main_page)
            await main_page.close()
            
            if not links:
                logger.warning("No links found, exiting...")
                await browser.close()
                return
            
            # Process links in batches
            all_results = []
            for i in range(0, len(links), MAX_CONCURRENT_SCRAPES):
                batch = links[i:i + MAX_CONCURRENT_SCRAPES]
                logger.info(f"\nProcessing batch {i//MAX_CONCURRENT_SCRAPES + 1}/{(len(links)-1)//MAX_CONCURRENT_SCRAPES + 1}")
                results = await scrape_batch(context, batch, i + 1)
                all_results.extend(results)
                
                # Rate limiting
                if i + MAX_CONCURRENT_SCRAPES < len(links):
                    await asyncio.sleep(1)
            
            # Write results
            if all_results:
                write_m3u8_file(all_results)
            else:
                logger.warning("No streams found to write")
            
            await browser.close()
            
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
    
    finally:
        elapsed = datetime.now() - start_time
        logger.info("=" * 80)
        logger.info(f"Scraper completed in {elapsed.total_seconds():.2f} seconds")
        logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())