#!/usr/bin/env python3
"""
Convert a Reddit /r/Cooking JSON dump to JSONL with cleaned fields.

Input can be:
  - A JSON array of threads (common for scraped dumps), or
  - JSONL (one JSON object per line).

Each output line looks like:
{
  "id": "<uuid4>",
  "language": "en",
  "source_url": "https://www.reddit.com/r/Cooking/comments/....",
  "thread_title": "...",
  "thread_body": "...",
  "replies_body": "...",
  "clean_status": "may_contain_spam",
  "subreddit": "Cooking"
}

Install (optional for language detection):
  pip install langid
"""

import argparse
import json
import re
import uuid
from pathlib import Path
from typing import Iterator, Dict, Any


# -------------------- Language detection --------------------
def detect_lang(text: str) -> str:
    """Detect language using langid if available; else fallback to 'en'/'und'."""
    try:
        import langid  # type: ignore
        lang, _ = langid.classify(text or "")
        return lang or "und"
    except Exception:
        # Fallback: assume English if there are Latin letters, else undetermined
        return "en" if text and re.search(r"[A-Za-z]", text) else "und"


# -------------------- Cleaning helpers --------------------
RE_EMOJI = re.compile(
    "["                                   # remove most emojis/stickers/symbols
    "\U0001F600-\U0001F64F"               # emoticons
    "\U0001F300-\U0001F5FF"               # symbols & pictographs
    "\U0001F680-\U0001F6FF"               # transport & map symbols
    "\U0001F1E0-\U0001F1FF"               # flags
    "\U00002700-\U000027BF"               # dingbats
    "\U0001F900-\U0001F9FF"               # supplemental symbols
    "\U00002600-\U000026FF"               # misc symbols
    "\U00002B00-\U00002BFF"               # arrows
    "]+"
)
RE_USER = re.compile(r"\bu/[A-Za-z0-9_-]+", re.IGNORECASE)
RE_URL  = re.compile(r"https?://\S+")
RE_EMAIL = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b")
RE_PHONE = re.compile(r"\b(?:\+?\d{1,3}[ -]?)?(?:\(?\d{2,4}\)?[ -]?)?\d{3,4}[ -]?\d{3,4}\b")

def clean_text(text: str) -> str:
    if not text:
        return ""
    # strip markdown blockquote markers
    text = re.sub(r"^\s*>\s*", "", text, flags=re.MULTILINE)
    # remove usernames, urls, emails, phones, emojis
    text = RE_USER.sub("", text)
    text = RE_URL.sub("", text)
    text = RE_EMAIL.sub("", text)
    text = RE_PHONE.sub("", text)
    text = RE_EMOJI.sub("", text)
    # unescape common HTML entities
    text = text.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&")
    # normalize whitespace
    text = re.sub(r"[^\S\r\n]+", " ", text)      # collapse spaces but keep newlines
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# -------------------- Thread / replies helpers --------------------
def combine_replies(replies) -> str:
    """Recursively collect all reply 'body' strings."""
    chunks = []
    if not replies:
        return ""
    for r in replies:
        body = r.get("body", "")
        if body:
            chunks.append(body)
        nested = r.get("replies", [])
        if isinstance(nested, list) and nested:
            chunks.append(combine_replies(nested))
    return "\n\n".join([c for c in chunks if c])


def full_reddit_url(relative: str) -> str:
    if not relative:
        return ""
    if relative.startswith("http"):
        return relative
    rel = relative if relative.startswith("/") else f"/{relative}"
    return f"https://www.reddit.com{rel}"


def extract_subreddit(url: str) -> str:
    m = re.search(r"/r/([^/]+)/", url)
    return m.group(1) if m else ""


def normalize_thread(thread_obj: Dict[str, Any]) -> Dict[str, Any]:
    # Handle common field variants
    title = thread_obj.get("title") or thread_obj.get("thread_title") or ""
    url   = thread_obj.get("url") or thread_obj.get("source_url") or ""
    body  = thread_obj.get("body") or thread_obj.get("thread_body") or ""
    comments = thread_obj.get("comments") or thread_obj.get("replies") or []

    thread_url = full_reddit_url(url)
    subreddit  = extract_subreddit(thread_url) or "Cooking"

    # Clean
    thread_title_clean = clean_text(title)
    thread_body_clean  = clean_text(body)
    replies_combined   = clean_text(combine_replies(comments))

    # Language (use title + body + replies for best signal)
    lang = detect_lang(f"{thread_title_clean}\n{thread_body_clean}\n{replies_combined}")

    return {
        "id": str(uuid.uuid4()),
        "language": lang,
        "source_url": thread_url,
        "thread_title": thread_title_clean,
        "thread_body": thread_body_clean,
        "replies_body": replies_combined,
        "clean_status": "may_contain_spam",
        "subreddit": subreddit,
    }


# -------------------- Input iterators (array, jsonl, tolerant) --------------------
def iter_threads_from_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
                    for t in obj["data"]:
                        if isinstance(t, dict):
                            yield t
                elif isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                continue


def iter_threads_from_json_array(path: Path) -> Iterator[Dict[str, Any]]:
    """Load a proper JSON array file."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        for t in data:
            if isinstance(t, dict):
                yield t
    elif isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        for t in data["data"]:
            if isinstance(t, dict):
                yield t


def tolerant_array_object_iter(path: Path) -> Iterator[Dict[str, Any]]:
    """
    Tolerantly scan a (possibly truncated) JSON array and yield top-level { ... } objects.
    Useful when the source file is a "copied/pasted smaller version" and not strictly valid JSON.
    """
    text = path.read_text(encoding="utf-8", errors="ignore")
    start = text.find('[')
    if start == -1:
        return
    i = start + 1
    depth = 0
    buf = []
    in_str = False
    esc = False
    while i < len(text):
        ch = text[i]
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
                buf.append(ch)
            elif ch == '{':
                depth += 1
                buf.append(ch)
            elif ch == '}':
                depth -= 1
                buf.append(ch)
                if depth == 0 and buf:
                    block = "".join(buf).strip()
                    buf.clear()
                    try:
                        obj = json.loads(block)
                        yield obj
                    except Exception:
                        pass
            else:
                if depth > 0:
                    buf.append(ch)
        i += 1


def smart_iter_threads(path: Path) -> Iterator[Dict[str, Any]]:
    """
    Try JSONL → JSON array → tolerant array parser (for imperfect files).
    """
    # Quick sniff
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        first = f.read(1)
    if first == "{":
        yield from iter_threads_from_jsonl(path)
        return
    # Try strict JSON array
    try:
        yield from iter_threads_from_json_array(path)
        return
    except Exception:
        pass
    # Fall back to tolerant parser
    yield from tolerant_array_object_iter(path)


# -------------------- Main --------------------
def main():
    ap = argparse.ArgumentParser(description="Convert cooking.json to cleaned JSONL.")
    ap.add_argument("input", help="Path to cooking.json (array) or JSONL")
    ap.add_argument("output", help="Path to write JSONL, e.g., cooking_clean.jsonl")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    n_in = n_out = 0
    with out_path.open("w", encoding="utf-8") as out:
        for raw in smart_iter_threads(in_path):
            n_in += 1
            norm = normalize_thread(raw)
            out.write(json.dumps(norm, ensure_ascii=False) + "\n")
            n_out += 1

    print(f"Processed {n_in} thread objects; wrote {n_out} JSONL lines to {out_path}")


if __name__ == "__main__":
    main()
