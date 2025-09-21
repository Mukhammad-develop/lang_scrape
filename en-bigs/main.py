import asyncio
import aiohttp
import aiofiles
import uuid
import json
import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# --------------------------------------
# CONFIG: Add more sites here
# --------------------------------------
SITES = {
    "bbcgoodfood": {
        "sitemap": "https://www.bbcgoodfood.com/sitemap.xml",
        "pattern": "/recipes/",
    },
    "jamieoliver": {
        "sitemap": "https://www.jamieoliver.com/sitemap.xml",
        "pattern": "/recipes/",
    },
    "nigella": {
        "sitemap": "https://www.nigella.com/sitemap.xml",
        "pattern": "/recipes/view/",
    },
    "rachaelray": {
        "sitemap": "https://www.rachaelraymag.com/sitemap.xml",
        "pattern": "/recipes/",
    },
    "pauladeen": {
        "sitemap": "https://www.pauladeen.com/sitemap_index.xml",
        "pattern": "/recipe/",
    },
    "waitrose": {
        "sitemap": "https://www.waitrose.com/sitemap.xml",
        "pattern": "/home/recipes/",
    },
    "marksandspencer": {
        "sitemap": "https://www.marksandspencer.com/sitemap.xml",
        "pattern": "/c/recipes/",
    },
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; RecipeBot/1.0)"}

# --------------------------------------
# Load already scraped URLs
# --------------------------------------
def load_done_urls(filepath):
    done = set()
    if not os.path.exists(filepath):
        return done
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                done.add(obj.get("source_url"))
            except Exception:
                continue
    return done

# --------------------------------------
# Fetch URL (with retries)
# --------------------------------------
async def fetch(session, url):
    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS, timeout=20) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception:
            await asyncio.sleep(1)
    return None

# --------------------------------------
# Parse sitemap into URLs
# --------------------------------------
async def parse_sitemap(session, sitemap_url, pattern):
    text = await fetch(session, sitemap_url)
    if not text:
        return []
    urls = set()
    try:
        root = ET.fromstring(text)
        for loc in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            url = loc.text.strip()
            if pattern in url:
                urls.add(url)
    except Exception:
        pass
    return list(urls)

# --------------------------------------
# Extract recipe JSON-LD
# --------------------------------------
def extract_recipe_data(html, url):
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD first
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string.strip())
            if isinstance(data, list):
                for d in data:
                    if d.get("@type") == "Recipe":
                        return d
            elif data.get("@type") == "Recipe":
                return data
        except Exception:
            continue

    # Fallback: Title + visible text
    title = soup.find("h1")
    content_blocks = soup.find_all(["p", "li"])
    text = "\n".join(cb.get_text(" ", strip=True) for cb in content_blocks)

    return {
        "name": title.get_text(strip=True) if title else "",
        "text": text,
    }

# --------------------------------------
# Save one recipe to JSONL
# --------------------------------------
async def save_recipe(site, url, recipe):
    if not recipe:
        return
    out = {
        "id": str(uuid.uuid4()),
        "lang": "en",
        "source_url": url,
        "title": recipe.get("name", ""),
        "text": recipe.get("description", recipe.get("text", "")),
        "clean_status": "clean",
        "category": "recipe",
    }
    async with aiofiles.open(f"{site}.jsonl", "a", encoding="utf-8") as f:
        await f.write(json.dumps(out, ensure_ascii=False) + "\n")

# --------------------------------------
# Crawl one site
# --------------------------------------
async def crawl_site(site, config, session):
    print(f"\n🔎 Crawling {site} ...")
    urls = await parse_sitemap(session, config["sitemap"], config["pattern"])
    urls = list(dict.fromkeys(urls))  # dedupe
    done_set = load_done_urls(f"{site}.jsonl")

    total = len(urls)
    counter = {"done": 0}

    print(f"   Found {total} candidate URLs, {len(done_set)} already done")

    sem = asyncio.Semaphore(10)  # limit concurrency per site

    async def process(url):
        if url in done_set:
            counter["done"] += 1
            print(f"[{counter['done']}/{total}] skipped {url}")
            return

        async with sem:
            html = await fetch(session, url)
            if not html:
                return
            recipe = extract_recipe_data(html, url)
            await save_recipe(site, url, recipe)

            counter["done"] += 1
            print(f"[{counter['done']}/{total}] saved {url}")

    await asyncio.gather(*(process(url) for url in urls))

# --------------------------------------
# Master runner
# --------------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*(crawl_site(site, cfg, session) for site, cfg in SITES.items()))

if __name__ == "__main__":
    asyncio.run(main())
