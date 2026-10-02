import contextlib
import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from scripts import register_schema
from scripts.register_schema import build_registration_payload


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas" / "cnc-demo-records.schema.json").read_text())


class SchemaRegistrationTests(unittest.TestCase):
    def test_payload_registers_the_canonical_schema_as_json_schema(self):
        payload = json.loads(build_registration_payload(json.dumps(SCHEMA)))
        self.assertEqual(payload["schemaType"], "JSON")
        self.assertEqual(json.loads(payload["schema"]), SCHEMA)

    def test_matching_subject_is_reused_without_posting(self):
        existing = {"id": 1, "schemaType": "JSON", "schema": json.dumps(SCHEMA)}
        with patch("scripts.register_schema.urlopen", return_value=io.BytesIO(json.dumps(existing).encode())) as opener:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(register_schema.main(), 0)
        self.assertEqual(opener.call_count, 1)
        self.assertIsInstance(opener.call_args.args[0], str)

    def test_missing_subject_is_registered_once(self):
        requests = []

        def open_request(request, timeout):
            requests.append(request)
            if isinstance(request, str):
                raise HTTPError(request, 404, "Not found", None, None)
            return io.BytesIO(b'{"id": 1}')

        with patch("scripts.register_schema.urlopen", side_effect=open_request):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(register_schema.main(), 0)
        self.assertEqual(len(requests), 2)
        self.assertIsInstance(requests[0], str)
        self.assertEqual(json.loads(requests[1].data)["schemaType"], "JSON")

    def test_mismatched_subject_fails_without_posting(self):
        existing = {"id": 1, "schemaType": "JSON", "schema": json.dumps({"type": "object"})}
        error_output = io.StringIO()
        with patch("scripts.register_schema.urlopen", return_value=io.BytesIO(json.dumps(existing).encode())) as opener:
            with contextlib.redirect_stderr(error_output):
                self.assertEqual(register_schema.main(), 1)
        self.assertEqual(opener.call_count, 1)
        self.assertIn("differs", error_output.getvalue())

    def test_unexpected_empty_response_fails_without_posting(self):
        error_output = io.StringIO()
        with patch("scripts.register_schema.urlopen", return_value=io.BytesIO(b"null")) as opener:
            with contextlib.redirect_stderr(error_output):
                self.assertEqual(register_schema.main(), 1)
        self.assertEqual(opener.call_count, 1)
        self.assertIn("unexpected response", error_output.getvalue())

    def test_registry_failure_returns_nonzero_with_actionable_error(self):
        error_output = io.StringIO()
        with patch("scripts.register_schema.urlopen", side_effect=OSError("registry unavailable")):
            with contextlib.redirect_stderr(error_output):
                self.assertEqual(register_schema.main(), 1)
        self.assertIn("Schema registration failed for cnc-demo-records-value", error_output.getvalue())

    def test_payload_rejects_non_object_schema(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            build_registration_payload("[]")


if __name__ == "__main__":
    unittest.main()
