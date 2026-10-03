import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from scripts.derive_event_avro_schema import derive_event_schema
from scripts.schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]


class EventAvroSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.canonical, cls.examples = parse_data_contract((ROOT / "DATA.md").read_text())
        cls.derived = derive_event_schema(cls.canonical)

    def test_event_schema_contains_canonical_envelope_and_payload_fields(self):
        fields = {field["name"]: field["type"] for field in self.derived["fields"]}
        self.assertEqual(
            set(fields),
            {"schema_version", "event_id", "kind", "occurred_at", "machine_id", "tool_instance_id", "payload"},
        )
        payload_fields = self.derived["fields"][-1]["type"]["fields"]
        self.assertEqual(
            {field["name"] for field in payload_fields},
            {"event_type", "cut_index", "component", "confirmed_fault", "planned", "replacement_tool_instance_id"},
        )

    def test_optional_event_fields_are_nullable_but_event_type_is_required(self):
        payload_fields = {field["name"]: field["type"] for field in self.derived["fields"][-1]["type"]["fields"]}
        self.assertEqual(payload_fields["event_type"], "string")
        for field in ("cut_index", "component", "confirmed_fault", "planned", "replacement_tool_instance_id"):
            with self.subTest(field=field):
                self.assertIn("null", payload_fields[field])

    def test_canonical_event_example_and_optional_installation_validate(self):
        validator = Draft202012Validator(self.canonical, format_checker=FormatChecker())
        example = next(record for record in self.examples if record.get("kind") == "event")
        validator.validate(example)

        installation = {
            "schema_version": "demo-1",
            "event_id": "install-cutter-1",
            "kind": "event",
            "occurred_at": "2026-03-04T10:15:00Z",
            "machine_id": "mill-1",
            "tool_instance_id": "cutter-1",
            "payload": {"event_type": "tool_installed"},
        }
        validator.validate(installation)

    def test_inspection_without_required_findings_is_rejected(self):
        validator = Draft202012Validator(self.canonical, format_checker=FormatChecker())
        inspection = {
            "schema_version": "demo-1",
            "event_id": "inspection-cutter-1",
            "kind": "event",
            "occurred_at": "2026-03-04T10:15:00Z",
            "machine_id": "mill-1",
            "tool_instance_id": "cutter-1",
            "payload": {"event_type": "inspection"},
        }
        self.assertFalse(validator.is_valid(inspection))

    def test_checked_in_event_schema_matches_canonical_derivation(self):
        schema_path = ROOT / "src" / "main" / "resources" / "avro" / "event-record.avsc"
        self.assertTrue(schema_path.is_file(), f"Missing generated schema asset: {schema_path}")
        self.assertEqual(json.loads(schema_path.read_text()), self.derived)
