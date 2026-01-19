import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

SEED_URL = "https://researchdosing.com/dosing-information/"
DOMAIN = "researchdosing.com"

# Anything that is clearly not a compound/blend page.
BLOCKED_SLUGS = {
    "dosing-information",
    "prep-injection-guide",
    "research-use-disclaimer",
    "about-us",
    "contact-us",
    "privacy-policy",
    "terms-of-service",
    "shipping-policy",
    "refund-policy",
    "returns",
    "faq",
    "faqs",
    "blog",
    "account",
    "my-account",
    "cart",
    "checkout",
    "search",
    "sitemap_index.xml",
}

@dataclass(frozen=True)
class LinkItem:
    url: str
    text: str

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ensure_run_dirs(run_date: str) -> Path:
    seeds_dir = Path("data") / "runs" / run_date / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)
    return seeds_dir

def _normalize_url(href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        if DOMAIN not in href:
            return ""
        return href.split("#")[0].rstrip("/")
    if href.startswith("/"):
        return f"https://{DOMAIN}{href}".split("#")[0].rstrip("/")
    return ""

def _slug(url: str) -> str:
    return url.replace(f"https://{DOMAIN}", "").strip("/")

def _is_candidate_compound_url(url: str) -> bool:
    if not url:
        return False

    path = url.replace(f"https://{DOMAIN}", "")
    # only allow one slug deep: /something
    if path.count("/") > 1:
        return False

    slug = _slug(url)
    if not slug:
        return False

    if slug in BLOCKED_SLUGS:
        return False

    # block common non-compound patterns
    if slug.startswith(("tag/", "category/", "product/", "page/")):
        return False
    if "?" in slug:
        return False

    # Heuristic: compound slugs are usually lowercase + numbers + hyphens, not "about-us" etc.
    # Keep permissive but exclude obvious site pages above.
    return True

def _fetch(url: str) -> requests.Response:
    headers = {
        "User-Agent": "Pep-Dose-AuditBot/1.0 (permissioned crawl; internal)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=45)
    resp.raise_for_status()
    return resp

def _extract_links(html: str) -> List[LinkItem]:
    soup = BeautifulSoup(html, "lxml")
    items: List[LinkItem] = []
    for a in soup.select("a[href]"):
        url = _normalize_url(a.get("href"))
        if not url:
            continue
        text = " ".join(a.get_text(" ", strip=True).split())
        items.append(LinkItem(url=url, text=text))
    return items

def _extract_taxonomy_best_effort(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    all_links = _extract_links(html)
    compound_urls = sorted({li.url for li in all_links if _is_candidate_compound_url(li.url)})

    groups: List[Dict[str, Any]] = []
    for h in soup.select("h2, h3"):
        heading = " ".join(h.get_text(" ", strip=True).split())
        if not heading:
            continue

        urls: List[str] = []
        node = h.next_sibling
        steps = 0
        while node is not None and steps < 250:
            steps += 1
            if getattr(node, "name", None) in ("h2", "h3"):
                break
            try:
                anchors = node.select("a[href]") if hasattr(node, "select") else []
            except Exception:
                anchors = []
            for a in anchors:
                u = _normalize_url(a.get("href"))
                if _is_candidate_compound_url(u):
                    urls.append(u)
            node = getattr(node, "next_sibling", None)

        # dedup preserve order
        seen: Set[str] = set()
        dedup: List[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                dedup.append(u)

        if len(dedup) >= 3:
            groups.append({"heading": heading, "urls": dedup})

    return {
        "detected_groups": groups,
        "all_candidate_compound_urls_on_seed_page": compound_urls,
        "note": "Best-effort grouping based on current DOM; the candidate URL list is the canonical seed set.",
    }

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    seeds_dir = _ensure_run_dirs(args.run_date)
    resp = _fetch(SEED_URL)
    html = resp.text

    (seeds_dir / "seed_page_snapshot.html").write_text(html, encoding="utf-8")

    links = _extract_links(html)
    candidates = sorted({li.url for li in links if _is_candidate_compound_url(li.url)})

    payload = {
        "seed_url": SEED_URL,
        "retrieved_at": _now_iso(),
        "http_status": resp.status_code,
        "num_total_links_found": len(links),
        "num_candidate_compound_urls": len(candidates),
        "candidate_compound_urls": candidates,
        "blocked_slugs": sorted(BLOCKED_SLUGS),
    }
    (seeds_dir / "seed_urls.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    taxonomy = _extract_taxonomy_best_effort(html)
    (seeds_dir / "site_taxonomy.json").write_text(json.dumps(taxonomy, indent=2), encoding="utf-8")

    print(f"OK seed_urls: {len(candidates)} candidate compound urls")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
