import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Iterable

CLAIM_TYPES = [
    "overview",
    "benefit",
    "mechanism",
    "contraindication",
    "warning",
    "reconstitution",
    "dosing_observed",
    "schedule_observed",
    "cycle_observed",
    "titration_observed",
    "stacking_observed",
    "storage",
    "side_effect",
    "other",
]

# regexes for numeric extraction
RE_NUM_UNIT = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>mcg|ug|mg|g|iu|units?|ml|mL)\b",
    re.IGNORECASE
)
RE_DURATION = re.compile(
    r"(?P<num>\d+)\s*(?P<unit>day|days|week|weeks|month|months)\b",
    re.IGNORECASE
)

def _load_parsed(run_date: str) -> List[Dict[str, Any]]:
    parsed_dir = Path("data") / "runs" / run_date / "parsed"
    items = []
    for p in sorted(parsed_dir.glob("*.json")):
        items.append(json.loads(p.read_text(encoding="utf-8")))
    return items

def _iter_text_units(block: Dict[str, Any]) -> Iterable[str]:
    t = block.get("type")
    if t == "paragraph":
        yield block.get("text", "")
    elif t == "raw_text":
        yield block.get("text", "")
    elif t == "list":
        for it in block.get("items", []) or []:
            yield it
    elif t == "table":
        for row in block.get("rows", []) or []:
            yield " | ".join(row)

def _extract_numbers(text: str) -> Dict[str, Any]:
    nums = []
    for m in RE_NUM_UNIT.finditer(text):
        nums.append({"value": float(m.group("num")), "unit": m.group("unit").lower()})
    durs = []
    for m in RE_DURATION.finditer(text):
        durs.append({"value": int(m.group("num")), "unit": m.group("unit").lower()})
    return {"numbers": nums, "durations": durs}

def _flags(text: str) -> List[str]:
    t = text.lower()
    flags = []
    if any(k in t for k in ["pregnant", "breastfeeding"]):
        flags.append("pregnancy_or_breastfeeding")
    if "cancer" in t:
        flags.append("cancer")
    if any(k in t for k in ["not a drug", "not for human consumption", "research-use only", "research use only", "not medical advice"]):
        flags.append("disclaimer_present")
    if any(k in t for k in ["prep & injection guide", "injection guide", "reconstitution", "bac water", "bacteriostatic"]):
        flags.append("injection_or_reconstitution_reference")
    if any(k in t for k in ["increase dose", "titrate", "intervals", "as necessary", "maximum dose"]):
        flags.append("titration_language")
    if any(k in t for k in ["stack", "stacking", "pairings", "suggested pairings"]):
        flags.append("stacking_language")
    return sorted(set(flags))

def _classify_line(text: str) -> str:
    t = text.lower()

    # hard matches first
    if any(k in t for k in ["contraindication", "do not take if", "do not use if"]):
        return "contraindication"
    if any(k in t for k in ["warning", "important:", "mistake", "risk", "danger"]):
        return "warning"
    if any(k in t for k in ["reconstitution", "mix with", "bac water", "bacteriostatic", "dilution", "reconstitute"]):
        return "reconstitution"
    if any(k in t for k in ["storage", "refriger", "room temperature", "freeze", "shelf life"]):
        return "storage"
    if any(k in t for k in ["side effects", "adverse", "nausea", "headache", "fatigue", "insomnia"]):
        return "side_effect"
    if any(k in t for k in ["cycle", "washout"]):
        return "cycle_observed"
    if any(k in t for k in ["daily", "weekly", "1x/day", "2x/day", "every", "per week"]):
        # could be schedule; keep as observed schedule
        return "schedule_observed"
    if any(k in t for k in ["dose", "units", "mcg", "mg", "iu", "ml"]):
        return "dosing_observed"
    if any(k in t for k in ["increase dose", "titrate", "intervals", "maximum dose"]):
        return "titration_observed"
    if any(k in t for k in ["stacking", "suggested pairings", "pairings"]):
        return "stacking_observed"
    if any(k in t for k in ["mechanism", "pathway", "receptor", "binds", "agonist", "antagonist"]):
        return "mechanism"
    if any(k in t for k in ["benefit", "helps", "improves", "useful in", "promotes", "supports"]):
        return "benefit"

    # fallback: if it’s long early paragraph treat as overview
    if len(text) >= 140:
        return "overview"
    return "other"

def _emit_claims_for_page(page: Dict[str, Any]) -> List[Dict[str, Any]]:
    slug = page["slug"]
    url = page["url"]
    sha = page.get("sha256", "")
    title = page.get("h1") or ""

    claims: List[Dict[str, Any]] = []
    claim_id = 0

    for sec in page.get("sections", []):
        path = sec.get("path", [])
        for block in sec.get("blocks", []):
            for text in _iter_text_units(block):
                text = (text or "").strip()
                if not text:
                    continue

                claim_type = _classify_line(text)
                claim_id += 1

                claims.append({
                    "id": f"{slug}:{claim_id}",
                    "source": {
                        "site": "researchdosing.com",
                        "url": url,
                        "slug": slug,
                        "sha256": sha,
                        "extracted_from": "html_snapshot",
                    },
                    "page_title": title,
                    "section_path": path,
                    "claim_type": claim_type,
                    "text": text,
                    "observed_not_guidance": True,
                    **_extract_numbers(text),
                    "flags": _flags(text),
                })

    return claims

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    pages = _load_parsed(args.run_date)

    claims_dir = Path("data") / "runs" / args.run_date / "claims"
    claims_dir.mkdir(parents=True, exist_ok=True)

    out_jsonl = claims_dir / "claims.jsonl"
    summary_path = Path("data") / "runs" / args.run_date / "reports" / "claims_summary.json"

    total = 0
    by_type: Dict[str, int] = {}
    by_flag: Dict[str, int] = {}
    by_slug: Dict[str, int] = {}

    with out_jsonl.open("w", encoding="utf-8") as f:
        for page in pages:
            page_claims = _emit_claims_for_page(page)
            for c in page_claims:
                f.write(json.dumps(c, ensure_ascii=False) + "\n")
                total += 1
                by_type[c["claim_type"]] = by_type.get(c["claim_type"], 0) + 1
                for fl in c.get("flags", []):
                    by_flag[fl] = by_flag.get(fl, 0) + 1
                slug = c["source"]["slug"]
                by_slug[slug] = by_slug.get(slug, 0) + 1

    summary = {
        "run_date": args.run_date,
        "claims_total": total,
        "claims_by_type": dict(sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0]))),
        "flags_counts": dict(sorted(by_flag.items(), key=lambda kv: (-kv[1], kv[0]))),
        "claims_by_slug_top10": sorted(by_slug.items(), key=lambda kv: -kv[1])[:10],
        "output": str(out_jsonl),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"OK extract_claims: claims={total}")
    print(f"Wrote: {out_jsonl}")
    print(f"Wrote: {summary_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
