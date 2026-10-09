"""Run the synthetic pipeline without installing Airflow."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from aurora_pipeline.pipeline import ENTITIES, sync_entity

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--start-date', default='2025-03-08')
parser.add_argument('--end-date', default='2025-03-10')
parser.add_argument('--entity', choices=['all'] + list(ENTITIES), default='all')
parser.add_argument('--db', default='output/warehouse.sqlite')
parser.add_argument('--dry-run', action='store_true')
args = parser.parse_args()
selected = ENTITIES if args.entity == 'all' else [args.entity]
results = [sync_entity(entity, args.start_date, args.end_date, args.dry_run, args.db) for entity in selected]
print(json.dumps(results, indent=2))
if not args.dry_run:
    with sqlite3.connect(args.db) as conn:
        count = conn.execute('SELECT count(*) FROM sqlite_master WHERE type="table"').fetchone()[0]
        print(f'Warehouse ready: {count} entity tables. Rerun to verify stable record counts.')
