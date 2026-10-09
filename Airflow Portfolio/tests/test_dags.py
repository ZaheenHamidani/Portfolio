"""Optional actual Airflow import check; run in the documented Airflow venv."""
import importlib.util
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))


@unittest.skipUnless(importlib.util.find_spec('airflow'), 'Airflow is not installed')
class DagTests(unittest.TestCase):
    def test_dagbag(self):
        from airflow.models import DagBag
        bag = DagBag(str(Path(__file__).resolve().parents[1] / 'dags'), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        self.assertEqual(set(bag.dag_ids), {'aurora_daily_sales', 'aurora_manual_backfill',
                                           'aurora_orders_only', 'aurora_daily_shifts'})
        manual = bag.get_dag('aurora_manual_backfill')
        self.assertEqual(manual.get_task('done').trigger_rule, 'none_failed_min_one_success')
        self.assertEqual(len(manual.get_task('choose_entities').downstream_task_ids), 6)
