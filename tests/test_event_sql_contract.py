import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVENT_SQL = ROOT / "scripts" / "init_events.sql"


class EventSqlContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = EVENT_SQL.read_text()
        cls.cut_sql = (ROOT / "scripts" / "init_cuts.sql").read_text()

    def test_reconciliation_table_has_unique_ids_and_a_default_unprocessed_flag(self):
        table = self.table_definition("mill_tool_event_reconciliation")
        self.assertRegex(table, r"event_id\s+TEXT\s+PRIMARY KEY")
        self.assertRegex(table, r"processed\s+BOOLEAN\s+NOT NULL\s+DEFAULT\s+FALSE")
        self.assertRegex(table, r"created_at\s+TIMESTAMPTZ\s+NOT NULL\s+DEFAULT\s+NOW\(\)")

    def test_entity_state_is_unique_per_tool_and_has_update_time(self):
        table = self.table_definition("mill_tool_event_state")
        self.assertRegex(table, r"PRIMARY KEY\s*\(\s*machine_id\s*,\s*tool_instance_id\s*\)")
        self.assertRegex(table, r"updated_at\s+TIMESTAMPTZ\s+NOT NULL\s+DEFAULT\s+NOW\(\)")
        self.assertRegex(table, r"payload\s+JSONB\s+NOT NULL")

    def test_event_bootstrap_is_additive_and_leaves_cut_tables_untouched(self):
        self.assertEqual(self.sql.count("CREATE TABLE IF NOT EXISTS"), 2)
        self.assertNotIn("DROP TABLE", self.sql.upper())
        self.assertIn("CREATE TABLE IF NOT EXISTS mill_cuts_transactional_writes", self.cut_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS mill_cuts_idempotent_writes", self.cut_sql)

    def test_repository_insert_does_not_reset_processed_rows_on_repeat(self):
        repository = (
            ROOT / "src" / "main" / "java" / "com" / "improving" / "gotogether" / "cuts"
            / "PostgresEventReconciliationRepository.java"
        ).read_text()
        self.assertIn("ON CONFLICT (event_id) DO NOTHING", repository)
        self.assertNotIn("processed = false", repository)

    def test_target_update_and_processed_marker_share_a_replay_safe_transaction(self):
        repository_path = (
            ROOT / "src" / "main" / "java" / "com" / "improving" / "gotogether" / "cuts"
            / "PostgresEventStateRepository.java"
        )
        repository = repository_path.read_text()
        apply_method = repository.split("public boolean applyIfUnprocessed", 1)[1].split("private static boolean isProcessed", 1)[0]
        self.assertIn("connection.setAutoCommit(false)", apply_method)
        self.assertLess(apply_method.index("upsertState(connection"), apply_method.index("markProcessed(connection"))
        marker_index = apply_method.index("markProcessed(connection")
        self.assertLess(marker_index, apply_method.index("connection.commit()", marker_index))
        self.assertIn("FOR UPDATE", repository)
        self.assertIn("updated_at = NOW()", repository)

    def table_definition(self, name):
        match = re.search(rf"CREATE TABLE IF NOT EXISTS {name}\s*\((.*?)\);", self.sql, re.DOTALL)
        self.assertIsNotNone(match, f"Missing CREATE TABLE IF NOT EXISTS {name}")
        return match.group(1)


if __name__ == "__main__":
    unittest.main()
