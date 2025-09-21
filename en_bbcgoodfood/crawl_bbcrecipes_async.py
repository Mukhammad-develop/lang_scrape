import asyncio
import aiohttp
import async_timeout
import json
import uuid
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from aiolimiter import AsyncLimiter

HEADERS = {"User-Agent": "MyRecipeCrawler/1.0 (+https://example.com)"}  # customize
CONCURRENCY = 6
REQUESTS_PER_SEC = 1   # polite default, lower if robots.txt demands
TIMEOUT = 20
OUTPUT = "bbc_recipes.jsonl"

# Check if URL looks like a recipe
def is_recipe_url(url: str) -> bool:
    return "/recipes/" in urlparse(url).path

# Fetch page text
async def fetch_text(session, url):
    async with async_timeout.timeout(TIMEOUT):
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 200:
                return await resp.text()
            return None

# Parse sitemap(s) recursively
async def parse_sitemap(session, sitemap_url):
    text = await fetch_text(session, sitemap_url)
    if not text:
        return []
    root = ET.fromstring(text.encode("utf-8"))
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    # handle sitemap index vs URL set
    if root.tag.endswith("sitemapindex"):
        for sitemap in root.findall("sm:sitemap", ns):
            loc = sitemap.find("sm:loc", ns)
            if loc is not None and loc.text:
                urls.extend(await parse_sitemap(session, loc.text))
    else:
        for url in root.findall("sm:url", ns):
            loc = url.find("sm:loc", ns)
            if loc is not None and loc.text:
                urls.append(loc.text)
    return urls

# Worker to fetch and save recipes
async def worker(name, session, queue, out_f, limiter, total, counter):
    while True:
        url = await queue.get()
        if url is None:
            queue.task_done()
            print(f"{name} exiting")
            break
        try:
            async with limiter:
                html = await fetch_text(session, url)
            if html:
                # Extract optional title
                soup = BeautifulSoup(html, "html.parser")
                page_title = soup.title.string.strip() if soup.title else ""

                # Build schema
                item = {
                    "id": str(uuid.uuid4()),
                    "lang": "en",
                    "source_url": url,
                    "title": page_title or "",
                    "text": "",   # keep blank for now
                    "clean_status": "clean",
                    "category": "recipe"
                }

                out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                out_f.flush()

                # progress
                counter["done"] += 1
                print(f"[{counter['done']}/{total}] saved {url}")

            await asyncio.sleep(0)
        except Exception as e:
            print("ERR", url, e)
        finally:
            queue.task_done()

# Main
async def main():
    sitemap_url = "https://www.bbcgoodfood.com/sitemap.xml"
    queue = asyncio.Queue()
    limiter = AsyncLimiter(REQUESTS_PER_SEC, 1)

    async with aiohttp.ClientSession() as session:
        print("Parsing sitemap...")
        all_urls = await parse_sitemap(session, sitemap_url)
        recipe_urls = [u for u in all_urls if is_recipe_url(u)]
        recipe_urls = list(dict.fromkeys(recipe_urls))  # dedupe
        total = len(recipe_urls)
        print("Found", total, "candidate recipe URLs (from sitemap).")

        # enqueue
        for u in recipe_urls:
            await queue.put(u)

        counter = {"done": 0}

        with open(OUTPUT, "w", encoding="utf-8") as out_f:
            # start workers
            workers = [
                asyncio.create_task(worker(f"w{i}", session, queue, out_f, limiter, total, counter))
                for i in range(CONCURRENCY)
            ]

            # add sentinels
            for _ in range(CONCURRENCY):
                await queue.put(None)

            # wait
            await queue.join()

            # close workers
            for w in workers:
                await w

if __name__ == "__main__":
    asyncio.run(main())
