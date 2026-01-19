import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

DOMAIN = "researchdosing.com"

def _load_manifest(run_date: str) -> List[Dict[str, Any]]:
    p = Path("data") / "runs" / run_date / "reports" / "raw_html_manifest.json"
    return json.loads(p.read_text(encoding="utf-8"))

def _clean(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = " ".join(s.split())
    return s.strip()

def _heading_level(tag: Tag) -> int:
    name = (tag.name or "").lower()
    if len(name) == 2 and name[0] == "h" and name[1].isdigit():
        return int(name[1])
    return 9

def _pick_container(soup: BeautifulSoup) -> Tag:
    return soup.find("main") or soup.find("article") or soup.body or soup

def _pick_best_title(container: Tag) -> str:
    # Prefer a non-brand h1 inside the container
    brand = {"research dosing"}
    h1s = []
    for h in container.find_all("h1"):
        t = _clean(h.get_text(" ", strip=True))
        if t and t.lower() not in brand:
            h1s.append(t)
    if h1s:
        return max(h1s, key=len)

    # Fallback: first h2/h3 that isn't brand
    for tag_name in ("h2", "h3"):
        h = container.find(tag_name)
        if h:
            t = _clean(h.get_text(" ", strip=True))
            if t and t.lower() not in brand:
                return t
    return ""

def _collect_blocks_between(start_h: Tag, end_h: Optional[Tag]) -> List[Dict[str, Any]]:
    """
    Walk DOM from start_h forward until end_h (exclusive), collecting:
    - paragraphs
    - lists (as list blocks)
    - tables
    """
    blocks: List[Dict[str, Any]] = []
    seen_lists = set()

    def add_paragraph(p: Tag):
        txt = _clean(p.get_text(" ", strip=True))
        if txt:
            blocks.append({"type": "paragraph", "text": txt})

    def add_list(lst: Tag):
        # prevent double-adding nested lists
        if id(lst) in seen_lists:
            return
        seen_lists.add(id(lst))
        items = []
        for li in lst.find_all("li"):
            t = _clean(li.get_text(" ", strip=True))
            if t:
                items.append(t)
        if items:
            blocks.append({"type": "list", "ordered": lst.name == "ol", "items": items})

    def add_table(table: Tag):
        rows = []
        for tr in table.find_all("tr"):
            cells = []
            for td in tr.find_all(["th", "td"]):
                c = _clean(td.get_text(" ", strip=True))
                if c:
                    cells.append(c)
            if cells:
                rows.append(cells)
        if rows:
            blocks.append({"type": "table", "rows": rows})

    for el in start_h.next_elements:
        if end_h is not None and el is end_h:
            break
        if not isinstance(el, Tag):
            continue

        name = (el.name or "").lower()

        # Stop if we hit ANY heading at same/higher boundary handled by caller
        # (caller already chooses end_h), so we ignore headings here.

        if name == "p":
            add_paragraph(el)
        elif name in ("ul", "ol"):
            add_list(el)
        elif name == "table":
            add_table(el)

    # If no structured blocks found, fallback to a compact text capture for the region
    if not blocks:
        txt = _clean(start_h.parent.get_text("\n", strip=True)) if start_h.parent else ""
        if txt:
            lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            if lines:
                blocks.append({"type": "raw_text", "text": "\n".join(lines[:200])})

    return blocks

def _parse_sections(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    container = _pick_container(soup)
    page_h1 = _pick_best_title(container)

    # headings inside container (ignore brand header)
    headings: List[Tag] = []
    for h in container.find_all(["h1", "h2", "h3", "h4"]):
        t = _clean(h.get_text(" ", strip=True))
        if not t:
            continue
        if t.lower() == "research dosing":
            continue
        headings.append(h)

    # If no headings, treat whole container as ROOT
    if not headings:
        txt = _clean(container.get_text("\n", strip=True))
        return {
            "title": title,
            "h1": page_h1,
            "sections": [{"path": ["ROOT"], "blocks": [{"type": "raw_text", "text": txt}]}],
        }

    # Build hierarchical paths + block regions
    sections: List[Dict[str, Any]] = []
    stack: List[Tuple[int, str]] = []

    for idx, h in enumerate(headings):
        level = _heading_level(h)
        heading_text = _clean(h.get_text(" ", strip=True))

        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, heading_text))
        path = [t for _, t in stack]

        # find next boundary heading of same or higher level
        end_h = None
        for j in range(idx + 1, len(headings)):
            if _heading_level(headings[j]) <= level:
                end_h = headings[j]
                break

        blocks = _collect_blocks_between(h, end_h)
        sections.append({"path": path, "blocks": blocks})

    return {"title": title, "h1": page_h1, "sections": sections}

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
