"""Offline ETL demonstrating pagination, transformation, and repeatable upserts."""
import json
import logging
import os
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
ENTITIES = ('customers', 'loyalty', 'categories', 'products', 'payments', 'orders', 'shifts')
DIMENSIONS = ('customers', 'loyalty', 'categories', 'products')
LOCAL_TZ = ZoneInfo('America/Edmonton')


def date_windows(start_date, end_date, chunk_days=7):
    """Inclusive business dates become contiguous, exclusive UTC boundaries."""
    begin, finish = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if finish < begin or chunk_days < 1:
        raise ValueError('Use an ordered date range and positive chunk_days')
    if (finish - begin).days > 365:
        raise ValueError('Demo backfills are limited to 366 calendar days')
    stop = finish + timedelta(days=1)
    while begin < stop:
        end = min(begin + timedelta(days=chunk_days), stop)
        yield (datetime.combine(begin, time.min, LOCAL_TZ).astimezone(timezone.utc),
               datetime.combine(end, time.min, LOCAL_TZ).astimezone(timezone.utc))
        begin = end


def previous_business_day(interval_end):
    """Use the logical run boundary so retries select the same calendar day."""
    if isinstance(interval_end, str):
        interval_end = datetime.fromisoformat(interval_end)
    if interval_end.tzinfo is None:
        raise ValueError('interval_end must have a timezone')
    return (interval_end.astimezone(LOCAL_TZ).date() - timedelta(days=1)).isoformat()


class MockPointOfSale:
    """Deterministic fictional records; no network clients or credentials."""
    def rows(self, entity, begin, end):
        if entity == 'customers':
            return [{'id': 'customer-demo-001', 'display_name': 'Demo Customer'}]
        if entity == 'loyalty':
            return [{'id': 'loyalty-demo-001', 'customer_id': 'customer-demo-001', 'points': 25}]
        if entity == 'categories':
            return [{'id': 'category-demo-001', 'name': 'Hot drinks'}]
        if entity == 'products':
            return [{'id': 'product-demo-001', 'category_id': 'category-demo-001', 'name': 'Aurora Latte'}]
        result = []
        day = begin.astimezone(LOCAL_TZ).date()
        while day < end.astimezone(LOCAL_TZ).date():
            suffix = day.isoformat()
            stamp = datetime.combine(day, time(10), LOCAL_TZ).astimezone(timezone.utc)
            if entity == 'payments':
                result.append({'id': f'payment-{suffix}', 'order_id': f'order-{suffix}',
                               'amount_cents': 1100, 'currency': 'CAD', 'business_date': suffix})
            elif entity == 'orders':
                result.append({'id': f'order-{suffix}', 'business_date': suffix,
                               'payment_id': f'payment-{suffix}', 'line_items': [
                                   {'uid': 'line-1', 'product_id': 'product-demo-001',
                                    'quantity': 2, 'gross_cents': 1000, 'tax_cents': 100,
                                    'discount_cents': 0, 'total_cents': 1100}]})
            elif entity == 'shifts':
                result.extend([
                    {'id': f'shift-{suffix}', 'employee_id': 'employee-demo-001',
                     'business_date': suffix, 'start_at': stamp.isoformat(),
                     'end_at': (stamp + timedelta(hours=8)).isoformat(), 'break_minutes': 30},
                    {'id': f'open-shift-{suffix}', 'employee_id': 'employee-demo-001',
                     'business_date': suffix, 'start_at': stamp.isoformat(), 'end_at': None}])
            day += timedelta(days=1)
        return result

    def pages(self, entity, begin, end, page_size=2):
        if page_size < 1:
            raise ValueError('page_size must be positive')
        rows = self.rows(entity, begin, end)
        for offset in range(0, len(rows), page_size):
            yield rows[offset:offset + page_size]


def transform(entity, record):
    if entity == 'orders':
        rows = []
        for item in record['line_items']:
            # Line IDs need the order namespace to avoid collisions between orders.
            key = str(uuid5(NAMESPACE_URL, f"aurora:{record['id']}:{item['uid']}"))
            rows.append({**item, 'id': key, 'order_id': record['id'],
                         'payment_id': record['payment_id'], 'business_date': record['business_date']})
        return rows
    if entity == 'shifts':
        if record.get('end_at') is None:
            return []
        if record['employee_id'] != 'employee-demo-001':
            raise ValueError('Shift references an unknown demo employee')
        duration = (datetime.fromisoformat(record['end_at']) -
                    datetime.fromisoformat(record['start_at'])).total_seconds() / 60
        paid = duration - record.get('break_minutes', 0)
        if paid < 0:
            raise ValueError('Shift has negative paid minutes')
        return [{**record, 'paid_minutes': int(paid)}]
    return [dict(record)]


class Warehouse:
    """One SQLite table per entity, with stable IDs and JSON business payloads."""
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def upsert(self, entity, rows):
        if entity not in ENTITIES:
            raise ValueError('Unknown entity')
        # Identifiers come exclusively from ENTITIES, never user input.
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute(f'CREATE TABLE IF NOT EXISTS {entity} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            conn.executemany(f'INSERT INTO {entity} VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                             [(row['id'], json.dumps(row, sort_keys=True)) for row in rows])


def sync_entity(entity, start_date, end_date, dry_run=False, db_path=None, chunk_days=7):
    if entity not in ENTITIES:
        raise ValueError('Unknown entity')
    # Resolve runtime configuration here; DAG parsing never opens a database.
    source = MockPointOfSale()
    warehouse = None if dry_run else Warehouse(db_path or os.getenv('DEMO_WAREHOUSE_PATH', '/tmp/aurora-demo/warehouse.sqlite'))
    processed = pages = 0
    for window_index, (begin, end) in enumerate(date_windows(start_date, end_date, chunk_days)):
        if entity in DIMENSIONS and window_index > 0:
            break  # Snapshot dimensions once, regardless of backfill length.
        for page in source.pages(entity, begin, end):
            pages += 1
            batch = {row['id']: row for record in page for row in transform(entity, record)}
            if warehouse is not None:
                warehouse.upsert(entity, list(batch.values()))
            processed += len(batch)
    result = {'entity': entity, 'rows_processed': processed, 'pages': pages, 'dry_run': dry_run}
    LOG.info('Sync summary: %s', result)
    return result
