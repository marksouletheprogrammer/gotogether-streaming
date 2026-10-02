import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from scripts.schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "DATA.md"
SCHEMA_PATH = ROOT / "schemas" / "cnc-demo-records.schema.json"


class SchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_text = DATA_PATH.read_text()
        cls.schema, cls.examples = parse_data_contract(cls.data_text)

    def test_data_contains_one_schema_and_three_examples(self):
        self.assertEqual(len(self.examples), 3)

    def test_parser_rejects_missing_or_duplicate_schema_blocks(self):
        with self.assertRaisesRegex(ValueError, "Expected exactly one JSON Schema block; found 0"):
            parse_data_contract('```json\n{}\n```')

        duplicate = f'{self.data_text}\n```json\n{json.dumps(self.schema)}\n```'
        with self.assertRaisesRegex(ValueError, "Expected exactly one JSON Schema block; found 2"):
            parse_data_contract(duplicate)

    def test_checked_in_asset_matches_canonical_schema(self):
        self.assertTrue(SCHEMA_PATH.is_file(), f"Missing generated schema asset: {SCHEMA_PATH}")
        self.assertEqual(json.loads(SCHEMA_PATH.read_text()), self.schema)

    def test_examples_and_invalid_records(self):
        schema = self.schema
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        self.assertEqual({example["kind"] for example in self.examples}, {"cut", "event", "assessment"})
        for example in self.examples:
            with self.subTest(kind=example["kind"]):
                validator.validate(example)

        cut = next(example for example in self.examples if example["kind"] == "cut")
        missing_reading = json.loads(json.dumps(cut))
        del missing_reading["payload"]["vibration_rms_g"]
        self.assertFalse(validator.is_valid(missing_reading))

        malformed_timestamp = json.loads(json.dumps(cut))
        malformed_timestamp["occurred_at"] = "not-a-timestamp"
        self.assertFalse(validator.is_valid(malformed_timestamp))


if __name__ == "__main__":
    unittest.main()
