#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IA crawler for practical daily-life topics using batched queries and wildcards.
Quick test:
  python main.py --lang eng --rows 80 --max-pages 1
Scale:
  python main.py --lang eng --rows 500 --max-pages 10
"""

import json, time, argparse
from pathlib import Path
from typing import List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter, Retry

OUT_DIR = Path("ia_tips")
JSONL_PATH = OUT_DIR / "tips.jsonl"
LOG_PATH = OUT_DIR / "run.log"

ADV_URL = "https://archive.org/advancedsearch.php"
META_URL = "https://archive.org/metadata/{identifier}"
DL_TMPL = "https://archive.org/download/{identifier}/{filename}"

DEFAULT_ROWS = 80
DEFAULT_MAX_PAGES = 1
DEFAULT_SLEEP = 0.25
DEFAULT_UA = "IA-practical-knowledge-crawler/1.1 (contact: your_email@example.com)"

RESTRICTED_COLLECTIONS = {"printdisabled", "inlibrary"}

# ===== Query batches (short & broad) =====
# Each entry becomes: mediatype:texts AND (subject:(...) OR title:(...) OR description:(...))
TERM_GROUPS = [
    # Cooking & food handling
    ["cook*", "cookery", "kitchen", "food prep*", "food handl*", "food safety"],
    # Preservation
    ["preserv*", "canning", "pickling", "ferment*", "dehydrat*", "freez*", "storage"],
    # Household & home economics
    ["home economics", "housekeeping", "household hints", "household tips", "home remedies"],
    # Cleaning, stains, odor
    ["clean*", "stain*", "odor*", "deodor*", "sanitation", "disinfect*"],
    # Object usage & manuals
    ["manual*", "handbook*", "guide*", "how to", "instruction", "use and care"],
    # Maintenance/repair/DIY
    ["mainten*", "repair", "DIY", "crafting", "do it yourself", "upcycling"],
    # Organization & placement
    ["organizat*", "declutter*", "object placement", "arrangement", "storage"],
    # Personal care & hygiene
    ["personal care", "hygiene", "personal hygiene", "first aid", "healthy alternative*"],
    # Drinks & seasoning
    ["beverage*", "juice*", "drink*", "season*", "spice*"],
    # Allergy subs
    ["allergy substitution*", "gluten-free", "dairy-free", "egg substitute"],
]

def make_session(ua: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(total=6, backoff_factor=0.7,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": ua})
    return s

def log(msg: str):
    print(msg, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def ia_advanced_search(session: requests.Session, q: str, rows: int, page: int, fields: List[str]) -> dict:
    params = {"q": q, "fl[]": fields, "rows": rows, "page": page, "output": "json"}
    r = session.get(ADV_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def ia_metadata(session: requests.Session, identifier: str) -> Optional[dict]:
    r = session.get(META_URL.format(identifier=identifier), timeout=60, allow_redirects=True)
    if r.status_code >= 400:
        return None
    return r.json()

def pick_text_file(files: List[dict]) -> Optional[dict]:
    cand: List[Tuple[int, dict]] = []
    for f in files:
        name = (f.get("name") or "").lower()
        fmt = (f.get("format") or "").lower()
        if name.endswith("_djvu.txt") or "djvu txt" in fmt or fmt == "txt":
            cand.append((0, f))
        elif name.endswith(".txt"):
            cand.append((1, f))
        elif "hocr" in fmt or name.endswith(".hocr"):
            cand.append((2, f))
        elif ("pdf" in fmt and "text" in fmt) or ("pdf text" in fmt):
            cand.append((3, f))
    if not cand: return None
    cand.sort(key=lambda x: x[0])
    return cand[0][1]

def download_text(session: requests.Session, identifier: str, fileobj: dict) -> Optional[str]:
    url = DL_TMPL.format(identifier=identifier, filename=fileobj["name"])
    r = session.get(url, timeout=120, allow_redirects=True)
    if r.status_code in (401,403,404):
        return None
    try:
        r.raise_for_status()
    except requests.HTTPError:
        return None
    return r.text.replace("\x00"," ")

def write_jsonl(record: dict):
    with open(JSONL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def norm_lang(lang_field) -> Optional[str]:
    if isinstance(lang_field, list) and lang_field:
        return str(lang_field[0])
    if isinstance(lang_field, str):
        return lang_field
    return None

def build_field_clause(terms: List[str], field: str) -> str:
    # e.g., subject:(cook* OR "home economics")
    parts = []
    for t in terms:
        if " " in t or "-" in t:
            parts.append(f'"{t}"')
        else:
            parts.append(t)
    return f'{field}:(' + " OR ".join(parts) + ")"

def build_query_for_terms(terms: List[str], lang: Optional[str], collections: Optional[str],
                          years: Optional[str], allow_restricted: bool) -> str:
    clauses = [ "mediatype:texts" ]
    # any of these fields may match
    field_or = "(" + " OR ".join([
        build_field_clause(terms, "subject"),
        build_field_clause(terms, "title"),
        build_field_clause(terms, "description"),
    ]) + ")"
    clauses.append(field_or)
    if lang:
        langs = " OR ".join(lang.split(","))
        clauses.append(f"language:({langs})")
    if collections:
        cols = " OR ".join(collections.split(","))
        clauses.append(f"collection:({cols})")
    if years:
        if "-" in years:
            y1,y2 = years.split("-",1)
            clauses.append(f"year:[{y1} TO {y2}]")
        else:
            clauses.append(f"year:{years}")
    if not allow_restricted:
        clauses.append("-collection:(printdisabled OR inlibrary)")
    return " AND ".join(clauses)

def crawl(args):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not JSONL_PATH.exists(): JSONL_PATH.touch()

    session = make_session(args.user_agent or DEFAULT_UA)
    fields = ["identifier","title","language","collection","year","subject","description"]

    total_written = 0
    for i, terms in enumerate(TERM_GROUPS, start=1):
        q = build_query_for_terms(terms, args.lang, args.collections, args.years, args.allow_restricted)
        log(f"\n[{i}/{len(TERM_GROUPS)}] Query: {q}")
        # page loop per query
        for page in range(1, (args.max_pages or 10**9) + 1):
            try:
                data = ia_advanced_search(session, q, args.rows, page, fields)
            except Exception as e:
                log(f"[ERROR] adv search failed (page {page}): {e}")
                time.sleep(args.sleep); continue

            resp = data.get("response", {})
            if page == 1:
                log(f"  numFound ≈ {resp.get('numFound', 0)}")

            docs = resp.get("docs", [])
            if not docs:
                if page == 1:
                    log("  No results for this query.")
                break

            written_this_page = 0
            for d in docs:
                identifier = d.get("identifier")
                if not identifier: continue

                if not args.allow_restricted:
                    cols = set(map(str.lower, (d.get("collection") or [])))
                    if RESTRICTED_COLLECTIONS & cols:
                        continue

                meta = ia_metadata(session, identifier)
                time.sleep(args.sleep)
                if not meta or "files" not in meta: continue

                fobj = pick_text_file(meta["files"])
                if not fobj: continue

                text = download_text(session, identifier, fobj)
                time.sleep(args.sleep)
                if not text: continue

                record = {
                    "id": identifier,
                    "title": d.get("title"),
                    "language": norm_lang(d.get("language")) or "unknown",
                    "year": d.get("year"),
                    "collections": d.get("collection"),
                    "subjects": d.get("subject"),
                    "source_url": f"https://archive.org/details/{identifier}",
                    "file_name": fobj.get("name"),
                    "file_format": fobj.get("format"),
                    "category": "practical_daily_life",
                    "clean_status": "raw_ocr",
                    "text": text,
                }
                write_jsonl(record)
                total_written += 1
                written_this_page += 1
                log(f"    [+] Saved {identifier}")

            log(f"  Page {page} done. Saved this page: {written_this_page}, total: {total_written}")
            if args.max_pages and page >= args.max_pages:
                break
            time.sleep(args.sleep)

    log(f"\nDone. Total records written: {total_written}")

def parse_args():
    p = argparse.ArgumentParser(description="IA crawler → JSONL (batched wildcard topics)")
    p.add_argument("--lang", type=str, default=None, help="Language filter, e.g. 'eng' or 'eng,rus'.")
    p.add_argument("--collections", type=str, default=None, help="Comma list of IA collections.")
    p.add_argument("--years", type=str, default=None, help="Year 'YYYY' or 'YYYY-YYYY'.")
    p.add_argument("--rows", type=int, default=DEFAULT_ROWS, help=f"Rows/page (default {DEFAULT_ROWS}).")
    p.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help=f"Max pages per query (default {DEFAULT_MAX_PAGES}).")
    p.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help=f"Delay between requests (default {DEFAULT_SLEEP}).")
    p.add_argument("--user-agent", type=str, default=DEFAULT_UA, help="Custom User-Agent.")
    p.add_argument("--allow-restricted", action="store_true", help="Include restricted (printdisabled/inlibrary).")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    crawl(args)
