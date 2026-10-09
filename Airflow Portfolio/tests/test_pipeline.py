import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from aurora_pipeline.pipeline import date_windows, previous_business_day, sync_entity, transform


class PipelineTests(unittest.TestCase):
    def test_dst_and_contiguous_chunks(self):
        windows = list(date_windows('2025-03-08', '2025-03-10', 1))
        self.assertEqual(len(windows), 3)
        self.assertEqual((windows[1][1] - windows[1][0]).total_seconds() / 3600, 23)
        self.assertEqual(windows[0][1], windows[1][0])
        self.assertEqual(windows[1][1], windows[2][0])
        fall = list(date_windows('2025-11-02', '2025-11-02'))
        self.assertEqual((fall[0][1] - fall[0][0]).total_seconds() / 3600, 25)

    def test_rerun_and_line_item_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / 'demo.sqlite')
            for _ in range(2):
                result = sync_entity('orders', '2025-03-08', '2025-03-10', db_path=db, chunk_days=1)
                self.assertEqual(result['rows_processed'], 3)
            with sqlite3.connect(db) as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM orders').fetchone()[0], 3)

    def test_dry_run_does_not_create_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / 'demo.sqlite'
            result = sync_entity('shifts', '2025-03-08', '2025-03-10', True, db)
            self.assertEqual(result['rows_processed'], 3)
            self.assertFalse(db.exists())

    def test_invalid_window_and_entity(self):
        with self.assertRaises(ValueError):
            list(date_windows('2025-03-10', '2025-03-08'))
        with self.assertRaises(ValueError):
            list(date_windows('2025-03-08', '2025-03-10', 0))
        with self.assertRaises(ValueError):
            sync_entity('unknown', '2025-03-08', '2025-03-08', True)

    def test_unknown_employee_and_paid_time(self):
        row = {'id': 'shift-demo', 'employee_id': 'employee-demo-001',
               'start_at': '2025-03-08T10:00:00+00:00',
               'end_at': '2025-03-08T18:00:00+00:00', 'break_minutes': 30}
        self.assertEqual(transform('shifts', row)[0]['paid_minutes'], 450)
        row['employee_id'] = 'unknown-demo'
        with self.assertRaises(ValueError):
            transform('shifts', row)

    def test_logical_boundary_is_repeatable(self):
        boundary = datetime.fromisoformat('2025-03-11T00:00:00+00:00')
        self.assertEqual(previous_business_day(boundary), '2025-03-09')


if __name__ == '__main__':
    unittest.main()
