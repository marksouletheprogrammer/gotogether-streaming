import json
import unittest
from pathlib import Path

from scripts.derive_cut_avro_schema import derive_cut_schema
from scripts.schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]


class AvroSchemaTests(unittest.TestCase):
    def test_checked_in_cut_schema_matches_canonical_json_schema_derivation(self):
        canonical_schema, _ = parse_data_contract((ROOT / "DATA.md").read_text())
        avro_path = ROOT / "src" / "main" / "resources" / "avro" / "cut-record.avsc"

        self.assertEqual(json.loads(avro_path.read_text()), derive_cut_schema(canonical_schema))
