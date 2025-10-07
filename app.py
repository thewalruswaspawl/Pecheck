import re
import time
from functools import lru_cache
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import streamlit as st

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_BASE = "https://en.wikipedia.org/wiki/"

# A very short seed list; you can expand this as you like.
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

# ---- Wikipedia utilities ----

@lru_cache(maxsize=256)
def wiki_search(query: str) -> Optional[str]:
    params = {
        "action": "opensearch",
        "search": query,
        "limit": 1,
        "namespace": 0,
        "format": "json"
    }
    r = requests.get(WIKI_API, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data and len(data) >= 2 and data[1]:
        return data[1][0]
    return None

@lru_cache(maxsize=256)
def wiki_page_html(title: str) -> str:
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|categories|links",
        "format": "json",
        "redirects": 1
    }
    r = requests.get(WIKI_API, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "parse" not in data:
        raise ValueError(f"Page not found: {title}")
    return data["parse"]["text"]["*"]

@lru_cache(maxsize=256)
def wiki_page_metadata(title: str) -> Dict:
    params = {
        "action": "parse",
        "page": title,
        "prop": "categories|links",
        "format": "json",
        "redirects": 1
    }
    r = requests.get(WIKI_API, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data.get("parse", {})

def extract_infobox(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    for cls in ["infobox vcard", "infobox", "infobox vevent", "infobox hproduct"]:
        tag = soup.find("table", {"class": lambda c: c and cls in c})
        if tag:
            return tag
    return None

def get_infobox_text_map(infobox: BeautifulSoup) -> Dict[str, str]:
    out = {}
    if not infobox:
        return out
    for row in infobox.find_all("tr"):
        header = row.find("th")
        data = row.find("td")
        if header and data:
            key = re.sub(r"\s+", " ", header.get_text(" ", strip=True)).lower()
            val = re.sub(r"\s+", " ", data.get_text(" ", strip=True))
            out[key] = val
    return out

def looks_like_company_page(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True).lower()
    return any(k in text for k in ["industry", "founded", "headquarters", "revenue", "number of employees"])

# ---- PE detection heuristics ----

def is_pe_owned_from_infobox(info: Dict[str, str]) -> Tuple[bool, str]:
    fields = ["owner", "owners", "parent company", "parent", "owner(s)", "key people", "owner(s)"]
    hay = " ".join([info.get(f, "") for f in fields]).lower()
    for kw in PE_KEYWORDS:
        if re.search(kw, hay, flags=re.IGNORECASE):
            return True, "Infobox ownership text indicates private equity."
    if any(any(pe in hay for pe in KNOWN_PE_FIRMS)):
        return True, "Infobox ownership lists a known PE firm."
    return False, ""

def is_pe_owned_from_body(text: str) -> Tuple[bool, str]:
    low = text.lower()
    for kw in PE_KEYWORDS:
        if re.search(kw, low, flags=re.IGNORECASE):
            return True, "Article text mentions private equity involvement."
    if any(pe in low for pe in KNOWN_PE_FIRMS):
        return True, "Article text names a known PE firm as owner/investor."
    if ("acquired" in low or "buyout" in low) and ("private" in low and "equity" in low):
        return True, "Article describes a PE acquisition."
    return False, ""

def detect_industry_categories(meta: Dict) -> Tuple[List[str], List[str]]:
    cats = []
    links = []
    for c in meta.get("categories", []):
        name = c.get("*")
        if name:
            cats.append(name)
    for l in meta.get("links", []):
        if l.get("ns") == 0 and l.get("*"):
            links.append(l.get("*"))
    industry_cats = [c for c in cats if any(token in c.lower() for token in ["companies", "manufacturers", "retail", "technology", "software", "telecommunications", "energy", "food", "beverage", "transport", "healthcare", "pharmaceutical", "financial", "bank", "insurance"])]
    return industry_cats, links

def extract_body_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", {"class": "mw-parser-output"})
    return re.sub(r"\s+", " ", content.get_text(" ", strip=True)) if content else soup.get_text(" ", strip=True)

@lru_cache(maxsize=512)
def get_page_pe_status(title: str) -> Dict:
    html = wiki_page_html(title)
    soup = BeautifulSoup(html, "html.parser")

    info = get_infobox_text_map(extract_infobox(soup))
    body_text = extract_body_text(html)

    pe_infobox, why1 = is_pe_owned_from_infobox(info)
    pe_body, why2 = is_pe_owned_from_body(body_text)
    is_pe = pe_infobox or pe_body
    reason = why1 or why2 or "No PE indicators found."

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

# ---- Peer discovery ----

def find_candidate_peers(seed_title: str, categories: List[str], limit: int = 40) -> List[str]:
    peers: List[str] = []

    top_cats = categories[:2] if categories else []
    for cat in top_cats:
        members = category_members(cat, max_items=limit//2)
        peers.extend(members)

    try:
        seed_html = wiki_page_html(seed_title)
        soup = BeautifulSoup(seed_html, "html.parser")
        for a in soup.select("a[href^='/wiki/']"):
            t = a.get("title") or ""
            if t.lower().startswith("list of") and "companies" in t.lower():
                peers.extend(list_page_companies(t, max_items=limit//2))
                break
    except Exception:
        pass

    dd = []
    seen = set()
    for p in peers:
        if p and p != seed_title and p not in seen:
            seen.add(p)
            dd.append(p)
    return dd[:limit]

def category_members(category_name: str, max_items: int = 20) -> List[str]:
    out = []
    cmcontinue = None
    tries = 0
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
        r = requests.get(WIKI_API, params=params, timeout=15)
        r.raise_for_status()
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

def list_page_companies(list_title: str, max_items: int = 20) -> List[str]:
    titles = []
    html = wiki_page_html(list_title)
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("div.mw-parser-output a[href^='/wiki/']"):
        t = a.get("title")
        if t and ":" not in t and not t.startswith("List of"):
            titles.append(t)
        if len(titles) >= max_items:
            break
    return titles

def filter_non_pe(peers: List[str], max_keep: int = 12) -> List[Dict]:
    results = []
    for t in peers:
        try:
            status = get_page_pe_status(t)
            html = wiki_page_html(t)
            soup = BeautifulSoup(html, "html.parser")
            if status["is_pe"]:
                continue
            if not looks_like_company_page(soup):
                continue
            results.append({"title": status["title"], "url": status["url"]})
            time.sleep(0.4)
            if len(results) >= max_keep:
                break
        except Exception:
            continue
    return results

# ---- UI ----

st.set_page_config(page_title="PE Ownership Checker", page_icon="💼", layout="centered")
st.title("💼 Private-Equity Ownership Checker")
st.caption("Powered by Wikipedia. Heuristics only—verify before making decisions.")

query = st.text_input("Company name", placeholder="e.g., Staples, Panera Bread, Epicor")
go = st.button("Check")

with st.expander("Settings"):
    max_peers = st.slider("Max alternate companies to return", 5, 25, 12)
    show_debug = st.checkbox("Show debug details (infobox fields, categories)")

if go and query.strip():
    with st.spinner("Searching Wikipedia..."):
        title = wiki_search(query.strip())
    if not title:
        st.error("No matching Wikipedia page found.")
        st.stop()

    try:
        status = get_page_pe_status(title)
        st.subheader(status["title"])
        st.write(f"[Open on Wikipedia]({status['url']})")

        if status["is_pe"]:
            st.markdown("### Ownership status: **Likely PE-owned/backed** ✅")
            st.caption(status["reason"])
        else:
            st.markdown("### Ownership status: **No clear PE indicators found** ❓")
            st.caption("This does **not** guarantee independence. Consider manual verification.")

        if show_debug:
            with st.expander("Infobox fields"):
                st.json(status["infobox"])
            with st.expander("Categories (subset)"):
                st.write(status["categories"][:12])

        st.markdown("---")
        st.markdown("### Non-PE peers (heuristic)")
        with st.spinner("Discovering peers..."):
            peers = find_candidate_peers(status["title"], status["categories"])
            non_pe = filter_non_pe(peers, max_keep=max_peers)

        if not non_pe:
            st.info("No clear non-PE peers found with current heuristics.")
        else:
            for p in non_pe:
                st.markdown(f"- [{p['title']}]({p['url']})")

    except Exception as e:
        st.error(f"Error: {e}")
        st.caption("Try a slightly different company name, or check your network.")
