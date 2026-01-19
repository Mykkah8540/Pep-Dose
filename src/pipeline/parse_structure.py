import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

DOMAIN = "researchdosing.com"

def _slug_from_url(url: str) -> str:
    return url.replace(f"https://{DOMAIN}/", "").strip("/")

def _load_manifest(run_date: str) -> List[Dict[str, Any]]:
    p = Path("data") / "runs" / run_date / "reports" / "raw_html_manifest.json"
    return json.loads(p.read_text(encoding="utf-8"))

def _clean(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = " ".join(s.split())
    return s.strip()

def _is_heading(tag_name: str) -> bool:
    return tag_name in ("h1", "h2", "h3", "h4")

def _heading_level(tag_name: str) -> int:
    return int(tag_name[1]) if tag_name.startswith("h") and tag_name[1:].isdigit() else 9

def _extract_blocks(node) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    # paragraphs
    for p in node.find_all(["p"], recursive=False):
        txt = _clean(p.get_text(" ", strip=True))
        if txt:
            blocks.append({"type": "paragraph", "text": txt})

    # lists
    for ul in node.find_all(["ul", "ol"], recursive=False):
        items = []
        for li in ul.find_all("li", recursive=False):
            t = _clean(li.get_text(" ", strip=True))
            if t:
                items.append(t)
        if items:
            blocks.append({"type": "list", "ordered": ul.name == "ol", "items": items})

    # tables (simple)
    for table in node.find_all("table", recursive=False):
        rows = []
        for tr in table.find_all("tr"):
            cells = [_clean(td.get_text(" ", strip=True)) for td in tr.find_all(["th", "td"])]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)
        if rows:
            blocks.append({"type": "table", "rows": rows})

    return blocks

def _parse_sections(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    # Strip scripts/styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag:
        h1 = _clean(h1_tag.get_text(" ", strip=True))

    container = soup.find("main") or soup.find("article") or soup.body or soup

    # Walk through headings in document order, build hierarchical section paths.
    sections: List[Dict[str, Any]] = []
    stack: List[Tuple[int, str]] = []  # (level, heading)

    # If there are no headings, treat container as one section
    headings = container.find_all(["h1", "h2", "h3", "h4"])
    if not headings:
        blocks = _extract_blocks(container)
        return {
            "title": title,
            "h1": h1,
            "sections": [{"path": ["ROOT"], "blocks": blocks}],
        }

    # Iterate through headings, capturing content until next heading of same/higher level
    for idx, h in enumerate(headings):
        level = _heading_level(h.name)
        heading_text = _clean(h.get_text(" ", strip=True))
        if not heading_text:
            continue

        # maintain stack
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, heading_text))
        path = [t for _, t in stack]

        # content nodes are siblings until next heading of same/higher level
        blocks: List[Dict[str, Any]] = []
        node = h.next_sibling
        steps = 0
        while node is not None and steps < 400:
            steps += 1
            name = getattr(node, "name", None)
            if isinstance(name, str) and _is_heading(name):
                if _heading_level(name) <= level:
                    break
            # if node is a Tag, attempt block extraction on it
            try:
                if hasattr(node, "find_all"):
                    blocks.extend(_extract_blocks(node))
            except Exception:
                pass
            node = getattr(node, "next_sibling", None)

        # If nothing captured, still emit section with empty blocks
        sections.append({"path": path, "blocks": blocks})

    return {"title": title, "h1": h1, "sections": sections}

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    manifest = _load_manifest(args.run_date)
    out_dir = Path("data") / "runs" / args.run_date / "parsed"
    out_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    for item in manifest:
        html_path = Path(item["html_file"])
        html = html_path.read_text(encoding="utf-8", errors="replace")
        parsed = _parse_sections(html)
        parsed["url"] = item["url"]
        parsed["slug"] = item["slug"]
        parsed["sha256"] = item["sha256"]
        (out_dir / f"{item['slug']}.json").write_text(json.dumps(parsed, indent=2), encoding="utf-8")
        written += 1

    print(f"OK parse_structure: wrote={written} parsed json files")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
