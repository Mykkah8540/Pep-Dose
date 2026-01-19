import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

DOMAIN = "researchdosing.com"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _slug_from_url(url: str) -> str:
    slug = url.replace(f"https://{DOMAIN}/", "").replace(f"http://{DOMAIN}/", "")
    slug = slug.strip("/").strip()
    slug = re.sub(r"[^a-zA-Z0-9\-]+", "-", slug)
    return slug or "index"

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _ensure_dirs(run_date: str) -> Dict[str, Path]:
    run_root = Path("data") / "runs" / run_date
    dirs = {
        "root": run_root,
        "raw_html": run_root / "raw_html",
        "text": run_root / "text",
        "reports": run_root / "reports",
        "seeds": run_root / "seeds",
    }
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return dirs

def _load_seed_urls(seeds_dir: Path) -> List[str]:
    seed_path = seeds_dir / "seed_urls.json"
    data = json.loads(seed_path.read_text(encoding="utf-8"))
    return list(data["candidate_compound_urls"])

def _fetch(url: str) -> Tuple[int, bytes, Dict[str, str]]:
    headers = {
        "User-Agent": "Pep-Dose-AuditBot/1.0 (permissioned crawl; internal)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=45)
    return r.status_code, r.content, {k: v for k, v in r.headers.items()}

def _extract_text_from_html(html: str) -> Tuple[str, str]:
    """
    Returns (title, text) as a readable extraction.
    Keep it deterministic and simple; later parse_structure will do section parsing.
    """
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(strip=True) if soup.title else ""
    # Remove script/style/noscript
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # Prefer main/article content if present
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = main.get_text("\n", strip=True)
    # collapse excess blank lines
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return title, "\n".join(lines)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    dirs = _ensure_dirs(args.run_date)
    urls = _load_seed_urls(dirs["seeds"])

    manifest: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for i, url in enumerate(urls, start=1):
        slug = _slug_from_url(url)
        try:
            status, content, headers = _fetch(url)
            html_path = dirs["raw_html"] / f"{slug}.html"
            html_path.write_bytes(content)

            html_hash = _sha256_bytes(content)
            title, text = _extract_text_from_html(content.decode("utf-8", errors="replace"))
            text_path = dirs["text"] / f"{slug}.txt"
            text_path.write_text(text, encoding="utf-8")

            manifest.append({
                "url": url,
                "slug": slug,
                "status": status,
                "retrieved_at": _now_iso(),
                "bytes": len(content),
                "sha256": html_hash,
                "title": title,
                "html_file": str(html_path),
                "text_file": str(text_path),
            })

            print(f"[{i:02d}/{len(urls)}] {status} {slug}")

        except Exception as e:
            failures.append({
                "url": url,
                "slug": slug,
                "error": repr(e),
                "retrieved_at": _now_iso(),
            })
            print(f"[{i:02d}/{len(urls)}] FAIL {slug}: {e}")

    (dirs["reports"] / "raw_html_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8"
    )
    (dirs["reports"] / "fetch_failures.json").write_text(
        json.dumps(failures, indent=2),
        encoding="utf-8"
    )

    print(f"OK snapshot_pages: fetched={len(manifest)} failed={len(failures)}")
    print(f"Wrote: {dirs['reports']/'raw_html_manifest.json'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
