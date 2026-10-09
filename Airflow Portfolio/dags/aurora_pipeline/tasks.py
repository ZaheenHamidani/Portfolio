"""Airflow task callables, kept separately from DAG construction."""
from datetime import datetime, timedelta
from .pipeline import ENTITIES, LOCAL_TZ, previous_business_day, sync_entity


def daily_entity(entity, data_interval_end, **context):
    day = previous_business_day(data_interval_end)
    return sync_entity(entity, day, day)


def manual_entity(entity, params, **context):
    return sync_entity(entity, params['start_date'], params['end_date'], params['dry_run'])


def choose_entities(params, **context):
    selected = params['entity']
    return [f'load_{e}' for e in ENTITIES if e != 'shifts'] if selected == 'all' else [f'load_{selected}']


def rolling_shifts(data_interval_end, params, **context):
    boundary = data_interval_end
    if isinstance(boundary, str):
        boundary = datetime.fromisoformat(boundary)
    end = boundary.astimezone(LOCAL_TZ).date() - timedelta(days=1)
    begin = end - timedelta(days=6)
    return sync_entity('shifts', begin.isoformat(), end.isoformat(), params['dry_run'])
