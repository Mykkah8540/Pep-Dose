import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

def _load_manifest(run_date: str) -> List[Dict[str, Any]]:
    p = Path("data") / "runs" / run_date / "reports" / "raw_html_manifest.json"
    return json.loads(p.read_text(encoding="utf-8"))

def _read_text(path_str: str) -> str:
    p = Path(path_str)
    return p.read_text(encoding="utf-8", errors="replace")

def _tokenize(text: str) -> List[str]:
    return re.findall(r"[A-Za-z0-9\-\+]+", text.lower())

def _score(text: str) -> Dict[str, Any]:
    tokens = _tokenize(text)
    uniq = len(set(tokens))
    total = len(tokens)

    # heuristic signals
    has_disclaimer = any(k in text.lower() for k in [
        "research use", "not medical advice", "educational", "disclaimer"
    ])
    has_reconstitution = any(k in text.lower() for k in [
        "reconstitution", "bacteriostatic", "bac water", "dilution", "reconstitute"
    ])
    has_storage = any(k in text.lower() for k in [
        "storage", "refriger", "room temperature", "shelf life", "freeze"
    ])
    has_measure = any(k in text.lower() for k in [
        "ml", "mg", "mcg", "iu", "units", "syringe"
    ])
    has_sections = sum(1 for k in [
        "overview", "mechanism", "benefits", "side effects", "warnings",
        "dosage", "dosing", "cycle", "administration", "reconstitution", "storage"
    ] if k in text.lower())

    # classification thresholds (tuned for this site’s page sizes)
    # These are intentionally conservative; we can adjust after first pass.
    if total < 600 or uniq < 220:
        label = "SHELL"
    elif total < 1200 or uniq < 420:
        label = "PARTIAL"
    else:
        label = "FULL"

    return {
        "token_total": total,
        "token_unique": uniq,
        "section_keyword_hits": has_sections,
        "has_disclaimer": has_disclaimer,
        "has_reconstitution": has_reconstitution,
        "has_storage": has_storage,
        "has_measurement_terms": has_measure,
        "label": label,
    }

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    manifest = _load_manifest(args.run_date)
    out_rows: List[Dict[str, Any]] = []

    for item in manifest:
        text = _read_text(item["text_file"])
        s = _score(text)
        out_rows.append({
            "slug": item["slug"],
            "url": item["url"],
            "bytes": item["bytes"],
            "status": item["status"],
            **s
        })

    # write JSON + CSV
    reports_dir = Path("data") / "runs" / args.run_date / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "page_classification.json").write_text(
        json.dumps(out_rows, indent=2), encoding="utf-8"
    )

    # CSV (manual writer to avoid pandas dependency)
    csv_path = reports_dir / "page_classification.csv"
    headers = list(out_rows[0].keys()) if out_rows else []
    lines = [",".join(headers)]
    for r in out_rows:
        vals = []
        for h in headers:
            v = r.get(h, "")
            if isinstance(v, bool):
                v = "1" if v else "0"
            vals.append(f"\"{str(v).replace('\"','\"\"')}\"")
        lines.append(",".join(vals))
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # print a quick summary
    from collections import Counter
    c = Counter(r["label"] for r in out_rows)
    print("OK classify_pages:", dict(c))
    print("Wrote:", csv_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
