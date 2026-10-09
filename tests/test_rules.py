"""
Unit tests for Airflow migration rules.
"""

import unittest
from airflow3_migrator.rules.imports import ImportsMigrationRule
from airflow3_migrator.rules.operators import OperatorsMigrationRule
from airflow3_migrator.rules.dag_params import DagParamsMigrationRule
from airflow3_migrator.rules.context_vars import ContextVarsMigrationRule
from airflow3_migrator.rules.datasets import DatasetToAssetMigrationRule
from airflow3_migrator.rules.db_access import DatabaseAccessMigrationRule
from airflow3_migrator.rules.config_rules import ConfigMigrationRule
from airflow3_migrator.rules.dependencies import DependenciesMigrationRule
from airflow3_migrator.rules.product_action import ProductActionMigrationRule
from airflow3_migrator.rules.sqlalchemy_rules import SQLAlchemyMigrationRule


class TestMigrationRules(unittest.TestCase):

    def test_imports_bash_and_python(self):
        rule = ImportsMigrationRule(dual_compat=False)
        code = "from airflow.operators.bash_operator import BashOperator\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)
        self.assertIn("airflow.providers.standard.operators.bash", issues[0].suggested_code)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("from airflow.providers.standard.operators.bash import BashOperator", fixed)

    def test_imports_dummy_operator_to_empty(self):
        rule = ImportsMigrationRule(dual_compat=False)
        code = "from airflow.operators.dummy_operator import DummyOperator\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)
        self.assertIn("EmptyOperator", issues[0].suggested_code)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("from airflow.providers.standard.operators.empty import EmptyOperator", fixed)


    def test_imports_get_current_context_to_sdk(self):
        rule = ImportsMigrationRule(dual_compat=False)
        # 1. Single import from airflow.operators.python
        code1 = "from airflow.operators.python import get_current_context\n"
        fixed1, _ = rule.fix(code1, "dag.py")
        self.assertEqual(fixed1, "from airflow.sdk import get_current_context\n")

        # 2. Combined import with PythonOperator
        code2 = "from airflow.operators.python import PythonOperator, get_current_context\n"
        fixed2, _ = rule.fix(code2, "dag.py")
        self.assertIn("from airflow.providers.standard.operators.python import PythonOperator", fixed2)
        self.assertIn("from airflow.sdk import get_current_context", fixed2)

        # 3. Import from airflow.utils.context
        code3 = "from airflow.utils.context import get_current_context\n"
        fixed3, _ = rule.fix(code3, "dag.py")
        self.assertEqual(fixed3, "from airflow.sdk import get_current_context\n")

    def test_imports_similar_sdk_and_provider_cases(self):
        rule = ImportsMigrationRule(dual_compat=False)

        # Decorators and TaskGroup to airflow.sdk
        code_dec = "from airflow.decorators import dag, task\n"
        fixed_dec, _ = rule.fix(code_dec, "dag.py")
        self.assertEqual(fixed_dec, "from airflow.sdk import dag, task\n")

        code_tg = "from airflow.utils.task_group import TaskGroup\n"
        fixed_tg, _ = rule.fix(code_tg, "dag.py")
        self.assertEqual(fixed_tg, "from airflow.sdk import TaskGroup\n")

        # ExternalTaskSensor to standard provider
        code_sensor = "from airflow.sensors.external_task import ExternalTaskSensor\n"
        fixed_sensor, _ = rule.fix(code_sensor, "dag.py")
        self.assertEqual(fixed_sensor, "from airflow.providers.standard.sensors.external_task import ExternalTaskSensor\n")

        # SubprocessHook to standard provider
        code_hook = "from airflow.hooks.subprocess import SubprocessHook\n"
        fixed_hook, _ = rule.fix(code_hook, "dag.py")
        self.assertEqual(fixed_hook, "from airflow.providers.standard.hooks.subprocess import SubprocessHook\n")

    def test_subdag_operator_flagged_critical(self):
        rule = ImportsMigrationRule()
        code = "from airflow.operators.subdag import SubDagOperator\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].rule_id, "AIR301_SUBDAG")
        self.assertFalse(issues[0].auto_fixable)

    def test_operators_dummy_operator_call_fixed(self):
        rule = OperatorsMigrationRule()
        code = "task1 = DummyOperator(task_id='start')\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("EmptyOperator(task_id='start')", fixed)

    def test_operators_provide_context_removed(self):
        rule = OperatorsMigrationRule()
        code = "task2 = PythonOperator(task_id='run', python_callable=fn, provide_context=True)\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertNotIn("provide_context", fixed)

    def test_dag_schedule_interval_to_schedule(self):
        rule = DagParamsMigrationRule(dual_compat=False)
        code = "with DAG(dag_id='test', schedule_interval='@daily') as dag:\n    pass\n"
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("schedule='@daily'", fixed)
        self.assertNotIn("schedule_interval", fixed)

    def test_context_vars_execution_date_to_logical_date(self):
        rule = ContextVarsMigrationRule(dual_compat=False)
        code = 'dt = kwargs["execution_date"]\ncmd = "echo {{ execution_date }}"\n'
        issues = rule.analyze(code, "dag.py")
        self.assertGreaterEqual(len(issues), 2)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn('kwargs["logical_date"]', fixed)
        self.assertIn('{{ logical_date }}', fixed)

    def test_datasets_to_assets(self):
        rule = DatasetToAssetMigrationRule(dual_compat=False)
        code = 'from airflow.datasets import Dataset\nds = Dataset("s3://bucket")\n'
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 2)

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("from airflow.sdk import Asset", fixed)
        self.assertIn('Asset("s3://bucket")', fixed)


    def test_db_access_metadata_flagged(self):
        rule = DatabaseAccessMigrationRule()
        # Direct Airflow metadata models are flagged
        code_airflow = "from airflow.models import DagRun\nres = session.query(DagRun).all()\n"
        issues = rule.analyze(code_airflow, "dag.py")
        self.assertGreaterEqual(len(issues), 1)
        self.assertEqual(issues[0].severity.value, "CRITICAL")
        self.assertFalse(issues[0].auto_fixable)

    def test_db_access_private_business_db_not_flagged(self):
        rule = DatabaseAccessMigrationRule()
        # Access to private business database / tables is NOT flagged
        code_private = (
            "from sqlalchemy.orm import Session\n"
            "from my_company.models import Client, Transaction\n"
            "session = Session(my_engine)\n"
            "clients = session.query(Client).filter(Client.active == True).all()\n"
        )
        issues = rule.analyze(code_private, "dag.py")
        self.assertEqual(len(issues), 0)

    def test_config_rules_xcom_and_sequential_executor(self):
        rule = ConfigMigrationRule()
        cfg = "executor = SequentialExecutor\nenable_xcom_pickling = True\n"
        issues = rule.analyze(cfg, "airflow.cfg")
        self.assertEqual(len(issues), 2)

        fixed, applied = rule.fix(cfg, "airflow.cfg")
        self.assertIn("LocalExecutor", fixed)
        self.assertIn("# SUPPRIMÉ EN AIRFLOW 3", fixed)

    def test_dependencies_rules(self):
        rule = DependenciesMigrationRule()
        reqs = "apache-airflow==2.8.1\nrequests>=2.0\nsqlalchemy==1.4.40\n"
        issues = rule.analyze(reqs, "requirements.txt")
        self.assertGreaterEqual(len(issues), 2)

        fixed, applied = rule.fix(reqs, "requirements.txt")
        self.assertIn("apache-airflow~=3.1.0", fixed)
        self.assertIn("apache-airflow-providers-standard>=1.0.0", fixed)
        self.assertIn("sqlalchemy>=2.0.0", fixed)

    def test_product_action_migration(self):
        rule = ProductActionMigrationRule()
        code = (
            "from datetime import datetime\n\n"
            '@product_action("xxx", payload=yyy, tags=["zzz"])\n'
            "def my_task():\n"
            "    pass\n"
        )
        issues = rule.analyze(code, "dag.py")
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].rule_id, "AIR310_PRODUCT_ACTION")

        fixed, applied = rule.fix(code, "dag.py")
        # Verify imports added
        self.assertIn("from pathlib import Path", fixed)
        self.assertIn("from bp2i_airflow_library.config import ENVIRONMENT", fixed)
        self.assertIn("from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS", fixed)
        # Verify action_id dynamic replacement
        self.assertIn('Path(__file__).stem.replace(".v1.", ".v2")', fixed)
        self.assertIn('if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")', fixed)
        self.assertIn("else Path(__file__).stem", fixed)
        self.assertIn("payload=yyy", fixed)
        self.assertIn('tags=["zzz"]', fixed)

        # Verify idempotency
        fixed_again, applied_again = rule.fix(fixed, "dag.py")
        self.assertEqual(len(applied_again), 0)
        self.assertEqual(fixed, fixed_again)

    def test_sqlalchemy_2_migration(self):
        rule = SQLAlchemyMigrationRule()
        code = (
            "from sqlalchemy.ext.declarative import declarative_base\n"
            "Base = declarative_base()\n"
            "conn.execute('SELECT id FROM users')\n"
        )
        issues = rule.analyze(code, "task.py")
        self.assertEqual(len(issues), 2)

        fixed, applied = rule.fix(code, "task.py")
        self.assertIn("from sqlalchemy.orm import declarative_base", fixed)
        self.assertIn("conn.execute(text('SELECT id FROM users'))", fixed)
        self.assertIn("from sqlalchemy import text", fixed)

        # Engine.execute is flagged as critical
        engine_code = "engine.execute('SELECT 1')\n"
        engine_issues = rule.analyze(engine_code, "task.py")
        self.assertEqual(len(engine_issues), 2)
        self.assertTrue(any(i.rule_id == "SQLA20_ENGINE_EXECUTE" for i in engine_issues))

    def test_dual_compat_imports(self):
        rule = ImportsMigrationRule(dual_compat=True)
        code = (
            "from airflow.operators.bash_operator import BashOperator\n"
            "from airflow.operators.python_operator import PythonOperator\n"
            "from airflow.operators.dummy_operator import DummyOperator\n"
            "from airflow.operators.python import get_current_context\n"
        )
        issues = rule.analyze(code, "dag.py")
        self.assertTrue(any(i.rule_id == "AIR302_DUAL_COMPAT" for i in issues))

        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("AIRFLOW_V_3_0_PLUS", fixed)
        self.assertIn("if AIRFLOW_V_3_0_PLUS:", fixed)
        self.assertIn("else:", fixed)
        # Check v3 branch
        self.assertIn("from airflow.providers.standard.operators.bash import BashOperator", fixed)
        self.assertIn("from airflow.providers.standard.operators.python import PythonOperator", fixed)
        self.assertIn("from airflow.providers.standard.operators.empty import EmptyOperator", fixed)
        self.assertIn("from airflow.sdk import get_current_context", fixed)
        # Check v2 branch
        self.assertIn("from airflow.operators.bash_operator import BashOperator", fixed)
        self.assertIn("from airflow.operators.python_operator import PythonOperator", fixed)
        self.assertIn("from airflow.operators.dummy_operator import DummyOperator, DummyOperator as EmptyOperator", fixed)
        self.assertIn("from airflow.operators.python import get_current_context", fixed)

    def test_dual_compat_dag_params(self):
        rule = DagParamsMigrationRule(dual_compat=True)
        code = (
            "from airflow import DAG\n"
            "dag = DAG('test', schedule_interval='0 0 * * *')\n"
        )
        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn('**({"schedule": \'0 0 * * *\'} if AIRFLOW_V_3_0_PLUS else {"schedule_interval": \'0 0 * * *\'})', fixed)
        self.assertIn("AIRFLOW_V_3_0_PLUS", fixed)

    def test_dual_compat_context_vars(self):
        rule = ContextVarsMigrationRule(dual_compat=True)
        code = (
            "def task_fn(**kwargs):\n"
            "    dt = kwargs['execution_date']\n"
        )
        fixed, applied = rule.fix(code, "dag.py")
        self.assertIn("(kwargs['logical_date'] if AIRFLOW_V_3_0_PLUS else kwargs['execution_date'])", fixed)
        self.assertIn("AIRFLOW_V_3_0_PLUS", fixed)


if __name__ == "__main__":
    unittest.main()

