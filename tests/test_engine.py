"""
Integration tests for MigrationEngine, BackupManager, and CLI flows.
"""

import unittest
import tempfile
import shutil
from pathlib import Path
from airflow3_migrator.engine import MigrationEngine
from airflow3_migrator.backup import BackupManager


class TestMigrationEngine(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.fixtures_dir = Path(__file__).parent / "fixtures"

        # Copy fixtures to temp directory for testing modifications
        shutil.copytree(self.fixtures_dir, Path(self.temp_dir) / "project")
        self.project_path = str(Path(self.temp_dir) / "project")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_analyze_fixtures_detects_issues(self):
        engine = MigrationEngine(self.project_path)
        summary = engine.analyze()

        self.assertGreater(summary.total_files_scanned, 0)
        self.assertGreater(summary.files_with_issues, 0)
        self.assertGreater(summary.total_issues, 0)
        self.assertGreater(summary.auto_fixable_issues, 0)
        self.assertGreater(summary.critical_issues, 0)

        # Check that diff is computed for modified files
        plans_with_diff = [p for p in summary.plans if p.diff]
        self.assertGreater(len(plans_with_diff), 0)

    def test_apply_and_rollback_workflow(self):
        engine = MigrationEngine(self.project_path)

        # 1. Apply changes
        summary = engine.apply(create_backup=True)
        self.assertIsNotNone(summary.backup_path)
        self.assertGreater(summary.files_modified, 0)

        # Verify DAG file was modified
        dag_file = Path(self.project_path) / "sample_dag_airflow2.py"
        with open(dag_file, "r", encoding="utf-8") as f:
            dag_content = f.read()

        self.assertIn("airflow.providers.standard.operators.bash", dag_content)
        self.assertIn("airflow.providers.standard.operators.empty", dag_content)
        self.assertIn('"schedule": "0 2 * * *"', dag_content)
        self.assertIn("AIRFLOW_V_3_0_PLUS", dag_content)
        self.assertIn("EmptyOperator", dag_content)
        self.assertIn("logical_date", dag_content)

        # 2. Rollback
        restored_count = engine.rollback()
        self.assertEqual(restored_count, summary.files_modified)

        # Verify DAG file was restored to original
        with open(dag_file, "r", encoding="utf-8") as f:
            restored_content = f.read()

        self.assertIn('schedule_interval="0 2 * * *"', restored_content)
        self.assertIn("DummyOperator", restored_content)


if __name__ == "__main__":
    unittest.main()
