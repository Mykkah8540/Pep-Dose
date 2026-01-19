import argparse

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    raise SystemExit(f"Stub not implemented yet: {__file__} (run-date={args.run_date})")

if __name__ == "__main__":
    raise SystemExit(main())
