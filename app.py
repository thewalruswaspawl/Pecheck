# PE Ownership Checker (Streamlit Cloud–friendly, hardened)
import re, time, random
from functools import lru_cache
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import streamlit as st

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_BASE = "https://en.wikipedia.org/wiki/"
USER_AGENT = "PEOwnershipChecker/1.0 (https://streamlit.app; contact: example@example.com)"

# ---------- HTTP helper with User-Agent + robust retry/backoff ----------
def _http_get(url, params=None, timeout=20, max_retries=5):
    """
    Polite Wikipedia requests with a UA and exponential backoff.
    Retries on 403/429/5xx and network exceptions.
    """
    backoff = 0.8
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                params=params or {},
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
            )
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            # Retry on rate limits & transient failures (include 403 for hosted environments)
            if status in (403, 429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= 1.8
                last_exc = e
                continue
            raise
        except requests.RequestException as e:
            # Network hiccup: retry
            time.sleep(backoff)
            backoff *= 1.8
            last_exc = e
            continue
    if last_exc:
        raise last_exc

# ---------- PE heuristics ----------
KNOWN_PE_FIRMS = {
    "blackstone", "kkr", "carlyle", "apollo global", "tpg capital", "advent international",
    "hellman & friedman", "warburg pincus", "vista equity", "silver lake", "thoma bravo",
    "platinium equity", "eqt", "permira", "bain capital", "gtcr", "leonard green",
    "genstar", "audax", "charterhouse", "bc partners", "clearlake capital", "sycamore partners",
    "sun capital", "centerbridge", "apax partners", "new mountain capital", "hellman and friedman"
}

PE_KEYWORDS = [
    r"private[-\s]?equity", r"leveraged buyout", r"LBO", r"buyout firm", r"PE-backed", r"PE backed",
    r"taken private", r"owner[s]?:?\s+[A-Z][A-Za-z&\s]+(Capital|Partners|Equity)"
]

# ---------- Wikipedia helpers ----------
@lru_cache(maxsize=256)
def wiki_search(query: str) -> Optional[str]:
    """Find the best Wikipedia page title for a company name; never raises."""
    # Try OpenSearch first
    try:
        params = {"action": "opensearch", "search": query, "limit": 1, "namespace": 0, "format": "json"}
        r = _http_get(WIKI_API, params=params, timeout=15)
        data = r.json()
        if data and len(data) >= 2 and data[1]:
            return data[1][0]
    except Exception:
        pass  # fall through

    # Fallback: 'query' search API
    try:
        params2 = {"action": "query", "list": "search", "srsearch": query, "srlimit": 1, "format": "json"}
        r2 = _http_get(WIKI_API, params=params2, timeout=20)
        data2 = r2.json()
        hits = data2.get("query", {}).get("search", [])
        if hits:
            return hits[0].get("title")
    except Exception:
        pass

    return None

@lru_cache(maxsize=256)
def wiki_page_html(title: str) -> str:
    params = {"action": "parse", "page": title, "prop": "text|categories|links", "format": "json", "redirects": 1}
    r = _http_get(WIKI_API, params=params, timeout=20)
    data = r.json()
    if "parse" not in data:
        raise ValueError(f"Page not found: {title}")
    return data["parse"]["text"]["*"]

@lru_cache(maxsize=256)
def wiki_page_metadata(title: str) -> Dict:
    params = {"action": "parse", "page": title, "prop": "categories|links", "format": "json", "redirects": 1}
    r = _http_get(WIKI_API, params=params, timeout=20)
    data = r.json()
    return data.get("parse", {})

# ---------- Parsing helpers ----------
def extract_infobox(soup: BeautifulSoup):
    for cls in ["infobox vcard", "infobox", "infobox vevent", "infobox hproduct"]:
        tag = soup.find("table", {"class": lambda c: c and cls in c})
        if tag:
            return tag
    return None

def get_infobox_text_map(infobox):
    out = {}
    if not infobox:
        return out
    for row in infobox.find_all("tr"):
        header, data = row.find("th"), row.find("td")
        if header and data:
            key = re.sub(r"\s+", " ", header.get_text(" ", strip=True)).lower()
            val = re.sub(r"\s+", " ", data.get_text(" ", strip=True))
            out[key] = val
    return out

def looks_like_company_page(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True).lower()
    return any(k in text for k in ["industry", "founded", "headquarters", "revenue", "number of employees"])

# ---------- PE detection ----------
def is_pe_owned_from_infobox(info):
    fields = ["owner", "owners", "parent company", "parent", "owner(s)", "key people"]
    hay = " ".join([info.get(f, "") for f in fields]).lower()
    for kw in PE_KEYWORDS:
        if re.search(kw, hay, re.I):
            return True, "Infobox indicates private equity."
    if any(pe in hay for pe in KNOWN_PE_FIRMS):
        return True, "Infobox lists a known PE firm."
    return False, ""

def is_pe_owned_from_body(text: str):
    low = text.lower()
    for kw in PE_KEYWORDS:
        if re.search(kw, low, re.I):
            return True, "Article mentions private equity."
    if any(pe in low for pe in KNOWN_PE_FIRMS):
        return True, "Article names a known PE firm."
    if ("acquired" in low or "buyout" in low) and ("private" in low and "equity" in low):
        return True, "Article describes a PE acquisition."
    return False, ""

def detect_industry_categories(meta: Dict):
    cats = [c.get("*") for c in meta.get("categories", []) if c.get("*")]
    links = [l.get("*") for l in meta.get("links", []) if l.get("ns") == 0 and l.get("*")]
    industry = [c for c in cats if any(t in c.lower() for t in [
        "companies", "manufacturers", "retail", "technology", "software", "telecommunications",
        "energy", "food", "beverage", "transport", "healthcare", "pharmaceutical", "financial", "bank", "insurance"
    ])]
    return industry, links

def extract_body_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", {"class": "mw-parser-output"})
    return re.sub(r"\s+", " ", content.get_text(" ", strip=True)) if content else soup.get_text(" ", strip=True)

# ---------- Main logic ----------
@lru_cache(maxsize=512)
def get_page_pe_status(title: str) -> Dict:
    html = wiki_page_html(title)
    soup = BeautifulSoup(html, "html.parser")

    info = get_infobox_text_map(extract_infobox(soup))
    body_text = extract_body_text(html)

    pe1, why1 = is_pe_owned_from_infobox(info)
    pe2, why2 = is_pe_owned_from_body(body_text)
    is_pe = pe1 or pe2
    reason = why1 or why2 or "No PE indicators."

    meta = wiki_page_metadata(title)
    cats, links = detect_industry_categories(meta)

    return {
        "title": title,
        "url": WIKI_BASE + title.replace(" ", "_"),
        "is_pe": is_pe,
        "reason": reason,
        "infobox": info,
        "categories": cats,
        "links": links
    }

def category_members(category_name: str, max_items: int = 20) -> List[str]:
    out, cmcontinue, tries = [], None, 0
    while len(out) < max_items and tries < 5:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_name}",
            "cmlimit": min(50, max_items - len(out)),
            "format": "json"
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        r = _http_get(WIKI_API, params=params, timeout=15)
        data = r.json()
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            if m.get("ns") == 0:
                out.append(m["title"])
        cmcontinue = data.get("continue", {}).get("cmcontinue")
        if not cmcontinue:
            break
        tries += 1
    return out

def find_candidate_peers(seed_title: str, categories: List[str], limit: int = 40) -> List[str]:
    peers = []
    top = categories[:2] if categories else []
    for cat in top:
        peers += category_members(cat, max_items=limit // 2)

    # Try to leverage "List of ... companies" pages linked from the article
    try:
        seed_html = wiki_page_html(seed_title)
        soup = BeautifulSoup(seed_html, "html.parser")
        for a in soup.select("a[href^='/wiki/']"):
            t = a.get("title") or ""
            if t.lower().startswith("list of") and "companies" in t.lower():
                # Quick scrape of company links from that list page
                try:
                    html = wiki_page_html(t)
                    soup_list = BeautifulSoup(html, "html.parser")
                    for a2 in soup_list.select("div.mw-parser-output a[href^='/wiki/']"):
                        tt = a2.get("title")
                        if tt and ":" not in tt and not tt.startswith("List of"):
                            peers.append(tt)
                            if len(peers) >= limit:
                                break
                except Exception:
                    pass
                break
    except Exception:
        pass

    # De-dup & remove seed
    dd, seen = [], set()
    for p in peers:
        if p and p != seed_title and p not in seen:
            seen.add(p)
            dd.append(p)
    return dd[:limit]

def filter_non_pe(peers: List[str], max_keep: int = 12) -> List[Dict]:
    res = []
    for t in peers:
        try:
            s = get_page_pe_status(t)
            html = wiki_page_html(t)
            soup = BeautifulSoup(html, "html.parser")
            if s["is_pe"]:
                continue
            if not looks_like_company_page(soup):
                continue
            res.append({"title": s["title"], "url": s["url"]})
            time.sleep(0.4)  # be gentle to the API
            if len(res) >= max_keep:
                break
        except Exception:
            continue
    return res

# ---------- Streamlit UI ----------
st.set_page_config(page_title="PE Ownership Checker", page_icon="💼", layout="centered")
st.title("💼 Private-Equity Ownership Checker")
st.caption("Heuristic Wikipedia-based ownership lookup. Verify results manually.")

query = st.text_input("Company name", placeholder="e.g., Staples, Panera Bread, Epicor")
go = st.button("Check")

with st.expander("Settings"):
    max_peers = st.slider("Max alternate companies", 5, 25, 12)
    show_debug = st.checkbox("Show debug details")

if go and query.strip():
    with st.spinner("Searching Wikipedia..."):
        title = wiki_search(query.strip())

    if not title:
        st.error("Couldn’t reach Wikipedia or no results found. Please try again in a minute.")
        st.caption("The app retries automatically, but Wikipedia may throttle hosted apps occasionally.")
        st.stop()

    try:
        status = get_page_pe_status(title)
        st.subheader(status["title"])
        st.write(f"[Open on Wikipedia]({status['url']})")

        if status["is_pe"]:
            st.markdown("### Ownership status: **Likely PE-owned/backed** ✅")
            st.caption(status["reason"])
        else:
            st.markdown("### Ownership status: **No clear PE indicators** ❓")
            st.caption("Not definitive — verify manually.")

        if show_debug:
            with st.expander("Infobox fields"):
                st.json(status["infobox"])
            with st.expander("Categories"):
                st.write(status["categories"][:12])

        st.markdown("---")
        st.markdown("### Non-PE peers (heuristic)")
        with st.spinner("Finding peers..."):
            peers = find_candidate_peers(status["title"], status["categories"])
            non_pe = filter_non_pe(peers, max_keep=max_peers)

        if not non_pe:
            st.info("No clear non-PE peers found.")
        else:
            for p in non_pe:
                st.markdown(f"- [{p['title']}]({p['url']})")

    except Exception as e:
        st.error(f"Error: {e}")
        st.caption("Try again or check your connection.")
