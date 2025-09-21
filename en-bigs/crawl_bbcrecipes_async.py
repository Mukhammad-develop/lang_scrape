import asyncio
import aiohttp
import async_timeout
import json
import uuid
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from aiolimiter import AsyncLimiter
import os

# -------------------------------
# CONFIG
# -------------------------------
HEADERS = {"User-Agent": "MyRecipeCrawler/1.0 (+https://example.com)"}
CONCURRENCY = 6
REQUESTS_PER_SEC = 1
TIMEOUT = 20
OUTPUT = "bbc_recipes.jsonl"

# -------------------------------
# Helpers
# -------------------------------
def is_recipe_url(url: str) -> bool:
    return "/recipes/" in urlparse(url).path

async def fetch_text(session, url):
    async with async_timeout.timeout(TIMEOUT):
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 200:
                return await resp.text()
            return None

async def parse_sitemap(session, sitemap_url):
    text = await fetch_text(session, sitemap_url)
    if not text:
        return []
    root = ET.fromstring(text.encode("utf-8"))
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
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

# -------------------------------
# Recipe extraction
# -------------------------------
def extract_recipe(html, url):
    soup = BeautifulSoup(html, "html.parser")

    # Try JSON-LD first
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string.strip())
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and d.get("@type") == "Recipe":
                        return {
                            "title": d.get("name", ""),
                            "text": (
                                d.get("description", "")
                                + "\n\nIngredients:\n"
                                + "\n".join(d.get("recipeIngredient", []))
                                + "\n\nMethod:\n"
                                + "\n".join(
                                    i if isinstance(i, str) else i.get("text", "")
                                    for i in d.get("recipeInstructions", [])
                                )
                            ).strip()
                        }
            elif isinstance(data, dict) and data.get("@type") == "Recipe":
                return {
                    "title": data.get("name", ""),
                    "text": (
                        data.get("description", "")
                        + "\n\nIngredients:\n"
                        + "\n".join(data.get("recipeIngredient", []))
                        + "\n\nMethod:\n"
                        + "\n".join(
                            i if isinstance(i, str) else i.get("text", "")
                            for i in data.get("recipeInstructions", [])
                        )
                    ).strip()
                }
        except Exception:
            continue

    # BBC Good Food fallback (manual HTML selectors)
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else ""

    # description
    desc = ""
    desc_block = soup.select_one("#recipe-masthead-description .editor-content p")
    if desc_block:
        desc = desc_block.get_text(" ", strip=True)

    # ingredients
    ingredients = []
    for li in soup.select("section#ingredients-list li"):
        ingredients.append(li.get_text(" ", strip=True))

    # method steps
    steps = []
    for li in soup.select("section.method-steps li"):
        steps.append(li.get_text(" ", strip=True))

    # merge into text
    text_parts = []
    if desc:
        text_parts.append(desc)
    if ingredients:
        text_parts.append("Ingredients:\n" + "\n".join(ingredients))
    if steps:
        text_parts.append("Method:\n" + "\n".join(steps))

    text = "\n\n".join(text_parts)

    return {
        "title": title,
        "text": text,
    }

# -------------------------------
# Worker
# -------------------------------
async def worker(name, session, queue, out_f, limiter, total, counter, done_set):
    while True:
        url = await queue.get()
        if url is None:
            queue.task_done()
            print(f"{name} exiting")
            break
        try:
            if url in done_set:
                counter["done"] += 1
                print(f"[{counter['done']}/{total}] skipped {url}")
                queue.task_done()
                continue

            async with limiter:
                html = await fetch_text(session, url)
            if html:
                recipe = extract_recipe(html, url)

                item = {
                    "id": str(uuid.uuid4()),
                    "lang": "en",
                    "source_url": url,
                    "title": recipe.get("title", ""),
                    "text": recipe.get("text", ""),
                    "clean_status": "clean",
                    "category": "recipe"
                }

                out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                out_f.flush()

                done_set.add(url)
                counter["done"] += 1
                print(f"[{counter['done']}/{total}] saved {url}")

            await asyncio.sleep(0)
        except Exception as e:
            print("ERR", url, e)
        finally:
            queue.task_done()

# -------------------------------
# Resume support
# -------------------------------
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

# -------------------------------
# Main
# -------------------------------
async def main():
    sitemap_url = "https://www.bbcgoodfood.com/sitemap.xml"
    queue = asyncio.Queue()
    limiter = AsyncLimiter(REQUESTS_PER_SEC, 1)

    done_set = load_done_urls(OUTPUT)
    print(f"Loaded {len(done_set)} already completed URLs")

    async with aiohttp.ClientSession() as session:
        print("Parsing sitemap...")
        all_urls = await parse_sitemap(session, sitemap_url)
        recipe_urls = [u for u in all_urls if is_recipe_url(u)]
        recipe_urls = list(dict.fromkeys(recipe_urls))
        total = len(recipe_urls)
        print("Found", total, "candidate recipe URLs")

        for u in recipe_urls:
            await queue.put(u)

        counter = {"done": 0}

        with open(OUTPUT, "a", encoding="utf-8") as out_f:
            workers = [
                asyncio.create_task(worker(f"w{i}", session, queue, out_f, limiter, total, counter, done_set))
                for i in range(CONCURRENCY)
            ]

            for _ in range(CONCURRENCY):
                await queue.put(None)

            await queue.join()

            for w in workers:
                await w

if __name__ == "__main__":
    asyncio.run(main())
