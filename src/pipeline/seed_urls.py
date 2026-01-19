import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
import requests
from bs4 import BeautifulSoup

SEED_URL = "https://researchdosing.com/dosing-information/"
DOMAIN = "researchdosing.com"

@dataclass(frozen=True)
class LinkItem:
    url: str
    text: str

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ensure_run_dirs(run_date: str) -> Tuple[Path, Path]:
    run_dir = Path("data") / "runs" / run_date / "seeds"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir, Path("data") / "runs" / run_date

def _normalize_url(href: str) -> str:
    href = href.strip()
    # ignore anchors/mailto/tel
    if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
        return ""
    # absolute internal
    if href.startswith("http://") or href.startswith("https://"):
        if DOMAIN not in href:
            return ""
        return href.split("#")[0].rstrip("/")
    # relative
    if href.startswith("/"):
        return f"https://{DOMAIN}{href}".split("#")[0].rstrip("/")
    # other relative forms
    return ""

def _is_candidate_compound_url(url: str) -> bool:
    # we want top-level slugs like /tirzepatide/ etc.
    # exclude obvious non-compound pages
    if not url:
        return False
    path = url.replace(f"https://{DOMAIN}", "")
    # must be 1 slug deep ("/something")
    if path.count("/") > 1:
        return False
    slug = path.strip("/")
    if not slug:
        return False
    blocked = {
        "dosing-information",
        "prep-injection-guide",
        "faq",
        "faqs",
        "contact",
        "about",
        "cart",
        "checkout",
        "my-account",
        "account",
        "privacy-policy",
        "terms",
        "shipping",
        "refund",
        "returns",
        "blog",
        "research-resources",
        "home",
    }
    if slug in blocked:
        return False
    # avoid obvious tag/search pages
    if slug.startswith("tag") or slug.startswith("category") or slug.startswith("product"):
        return False
    return True

def _fetch(url: str) -> requests.Response:
    headers = {
        "User-Agent": "Pep-Dose-AuditBot/1.0 (permissioned crawl; contact: internal)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=45)
    resp.raise_for_status()
    return resp

def _extract_links(html: str) -> List[LinkItem]:
    soup = BeautifulSoup(html, "lxml")
    items: List[LinkItem] = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        url = _normalize_url(href)
        if not url:
            continue
        text = " ".join(a.get_text(" ", strip=True).split())
        items.append(LinkItem(url=url, text=text))
    return items

def _extract_taxonomy(html: str) -> Dict[str, Any]:
    """
    Best-effort extraction of visible category groupings.
    Because the site markup may change, we store a flexible structure:
    - headings and the urls underneath each heading (based on DOM proximity)
    If we can't reliably segment, we still preserve:
    - all headings (h2/h3)
    - all candidate compound urls found on the page
    """
    soup = BeautifulSoup(html, "lxml")

    # collect candidate compound links on the seed page
    all_links = _extract_links(html)
    compound_links = [li for li in all_links if _is_candidate_compound_url(li.url)]

    # attempt grouping by headings: for each H2/H3, scan until next heading of same or higher level
    headings = []
    for h in soup.select("h2, h3"):
        title = " ".join(h.get_text(" ", strip=True).split())
        if not title:
            continue
        headings.append(h)

    groups: List[Dict[str, Any]] = []
    for h in headings:
        title = " ".join(h.get_text(" ", strip=True).split())
        # walk siblings until next h2/h3
        urls: List[str] = []
        node = h.next_sibling
        steps = 0
        while node is not None and steps < 200:
            steps += 1
            # stop if we hit another heading
            if getattr(node, "name", None) in ("h2", "h3"):
                break
            # if it's a tag with links, capture
            try:
                anchors = node.select("a[href]") if hasattr(node, "select") else []
            except Exception:
                anchors = []
            for a in anchors:
                url = _normalize_url(a.get("href") or "")
                if _is_candidate_compound_url(url):
                    urls.append(url)
            node = getattr(node, "next_sibling", None)

        # de-dup while preserving order
        seen: Set[str] = set()
        urls_dedup = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                urls_dedup.append(u)

        # keep groups that look like real categories (at least 3 urls)
        if len(urls_dedup) >= 3:
            groups.append({"heading": title, "urls": urls_dedup})

    return {
        "detected_groups": groups,
        "all_candidate_compound_urls_on_seed_page": sorted({li.url for li in compound_links}),
        "note": "Grouping is best-effort based on current DOM structure; rely on candidate URL list as canonical seed set.",
    }

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    seeds_dir, run_root = _ensure_run_dirs(args.run_date)

    resp = _fetch(SEED_URL)
    html = resp.text

    # Save snapshot for audit
    (seeds_dir / "seed_page_snapshot.html").write_text(html, encoding="utf-8")

    links = _extract_links(html)
    # Candidate compound pages
    candidates = sorted({li.url for li in links if _is_candidate_compound_url(li.url)})

    payload = {
        "seed_url": SEED_URL,
        "retrieved_at": _now_iso(),
        "http_status": resp.status_code,
        "num_total_links_found": len(links),
        "num_candidate_compound_urls": len(candidates),
        "candidate_compound_urls": candidates,
    }
    (seeds_dir / "seed_urls.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    taxonomy = _extract_taxonomy(html)
    (seeds_dir / "site_taxonomy.json").write_text(json.dumps(taxonomy, indent=2), encoding="utf-8")

    print(f"OK seed_urls: {len(candidates)} candidate compound urls")
    print(f"Wrote: {seeds_dir/'seed_urls.json'}")
    print(f"Wrote: {seeds_dir/'site_taxonomy.json'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
