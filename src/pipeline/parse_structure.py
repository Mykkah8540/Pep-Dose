import argparse
import json
import re
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

def _heading_level(name: str) -> int:
    if name and name.startswith("h") and len(name) == 2 and name[1].isdigit():
        return int(name[1])
    return 9

def _pick_container(soup: BeautifulSoup) -> Tag:
    # Prefer main/article; if missing, fallback to body.
    return soup.find("main") or soup.find("article") or soup.body or soup

def _pick_best_h1(container: Tag) -> str:
    # Many themes have a site header H1 ("Research Dosing") plus a page title elsewhere.
    # Choose the longest non-empty H1 that isn't the brand.
    brand = {"research dosing"}
    candidates = []
    for h in container.find_all("h1"):
        t = _clean(h.get_text(" ", strip=True))
        if not t:
            continue
        if t.lower() in brand:
            continue
        candidates.append(t)
    if candidates:
        return max(candidates, key=len)

    # fallback: some pages use h2/h3 for title
    for sel in ["h2", "h3"]:
        h = container.find(sel)
        if h:
            t = _clean(h.get_text(" ", strip=True))
            if t and t.lower() not in brand:
                return t
    return ""

def _extract_blocks_recursive(node: Tag) -> List[Dict[str, Any]]:
    """
    Extract paragraphs, lists, and tables recursively within node.
    Keep ordering coarse but deterministic.
    """
    blocks: List[Dict[str, Any]] = []

    # paragraphs
    for p in node.find_all("p"):
        txt = _clean(p.get_text(" ", strip=True))
        if txt:
            blocks.append({"type": "paragraph", "text": txt})

    # lists
    for ul in node.find_all(["ul", "ol"]):
        items = []
        for li in ul.find_all("li"):
            t = _clean(li.get_text(" ", strip=True))
            if t:
                items.append(t)
        if items:
            blocks.append({"type": "list", "ordered": ul.name == "ol", "items": items})

    # tables
    for table in node.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [_clean(td.get_text(" ", strip=True)) for td in tr.find_all(["th", "td"])]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)
        if rows:
            blocks.append({"type": "table", "rows": rows})

    # If we got nothing, fallback to raw text snippet (prevents empty sections)
    if not blocks:
        txt = _clean(node.get_text("\n", strip=True))
        if txt:
            # collapse into manageable lines
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
    h1 = _pick_best_h1(container)

    # headings within container (skip brand h1 if present)
    headings = []
    for h in container.find_all(["h1", "h2", "h3", "h4"]):
        t = _clean(h.get_text(" ", strip=True))
        if not t:
            continue
        if t.lower() == "research dosing":
            continue
        headings.append(h)

    # If we can't find headings, treat whole container as one root section
    if not headings:
        return {
            "title": title,
            "h1": h1,
            "sections": [{"path": ["ROOT"], "blocks": _extract_blocks_recursive(container)}],
        }

    sections: List[Dict[str, Any]] = []
    stack: List[Tuple[int, str]] = []

    for h in headings:
        level = _heading_level(h.name or "")
        heading_text = _clean(h.get_text(" ", strip=True))

        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, heading_text))
        path = [t for _, t in stack]

        # Collect content until next heading of same/higher level
        region_nodes: List[Tag] = []
        node = h.next_sibling
        steps = 0
        while node is not None and steps < 800:
            steps += 1
            name = getattr(node, "name", None)
            if isinstance(name, str) and name in ("h1", "h2", "h3", "h4"):
                nxt_level = _heading_level(name)
                if nxt_level <= level:
                    break
            if isinstance(node, Tag):
                region_nodes.append(node)
            node = getattr(node, "next_sibling", None)

        # Wrap region nodes into a temporary container for extraction
        tmp = BeautifulSoup("<div></div>", "lxml").div
        for n in region_nodes:
            try:
                tmp.append(n)
            except Exception:
                pass

        blocks = _extract_blocks_recursive(tmp)
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
