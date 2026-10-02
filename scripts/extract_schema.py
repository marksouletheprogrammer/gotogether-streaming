import json
from pathlib import Path

from schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "DATA.md"
SCHEMA_PATH = ROOT / "schemas" / "cnc-demo-records.schema.json"


def main():
    try:
        schema, _ = parse_data_contract(DATA_PATH.read_text())
    except ValueError as error:
        raise SystemExit(f"Cannot extract schema from {DATA_PATH}: {error}") from error

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(json.dumps(schema, indent=2) + "\n")


if __name__ == "__main__":
    main()
