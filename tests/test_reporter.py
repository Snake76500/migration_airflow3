"""
Tests for MigrationReporter (HTML, Markdown, JSON, Console).
"""

import json
import unittest
from pathlib import Path
from airflow3_migrator.engine import MigrationEngine
from airflow3_migrator.reporter import MigrationReporter


class TestMigrationReporter(unittest.TestCase):

    def setUp(self):
        fixtures_dir = Path(__file__).parent / "fixtures"
        self.engine = MigrationEngine(str(fixtures_dir))
        self.summary = self.engine.analyze()
        self.reporter = MigrationReporter(self.summary)

    def test_generate_json(self):
        json_output = self.reporter.generate_json()
        data = json.loads(json_output)
        self.assertIn("summary", data)
        self.assertIn("files", data)
        self.assertGreater(data["summary"]["total_issues"], 0)

    def test_generate_markdown(self):
        md_output = self.reporter.generate_markdown()
        self.assertIn("# 🚀 Rapport de Migration : Airflow 2 ➔ Airflow 3", md_output)
        self.assertIn("Statistiques Générales", md_output)
        self.assertIn("```diff", md_output)

    def test_generate_html(self):
        html_output = self.reporter.generate_html()
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("Airflow 2 ➔ Airflow 3 Migration", html_output)
        self.assertIn("diff-container", html_output)

    def test_console_output_no_crash(self):
        # Verify console summary does not throw exceptions
        self.reporter.print_console_summary(show_diffs=True, use_color=False)
        self.reporter.print_console_summary(show_diffs=False, use_color=True)


if __name__ == "__main__":
    unittest.main()
