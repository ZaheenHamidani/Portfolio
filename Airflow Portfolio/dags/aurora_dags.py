"""Fictional Aurora Café DAGs; designed for Apache Airflow 2.10.5."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from aurora_pipeline.pipeline import ENTITIES
from aurora_pipeline.tasks import choose_entities, daily_entity, manual_entity, rolling_shifts

DEFAULTS = {'owner': 'demo_data_team', 'retries': 2,
            'retry_delay': timedelta(minutes=5), 'execution_timeout': timedelta(minutes=30)}
START = pendulum.datetime(2025, 1, 1, tz='America/Edmonton')
SALES = [e for e in ENTITIES if e != 'shifts']


def manual_params():
    return {'start_date': Param('2025-03-08', type='string', format='date'),
            'end_date': Param('2025-03-10', type='string', format='date'),
            'dry_run': Param(False, type='boolean')}


with DAG('aurora_daily_sales', schedule='0 18 * * *', start_date=START,
         catchup=False, max_active_runs=1, default_args=DEFAULTS,
         tags=['portfolio', 'daily', 'synthetic']) as daily_sales:
    dimensions = [PythonOperator(task_id=f'load_{entity}', python_callable=daily_entity,
                                op_kwargs={'entity': entity}) for entity in SALES[:4]]
    payments = PythonOperator(task_id='load_payments', python_callable=daily_entity,
                              op_kwargs={'entity': 'payments'})
    orders = PythonOperator(task_id='load_orders', python_callable=daily_entity,
                            op_kwargs={'entity': 'orders'})
    dimensions >> payments >> orders

with DAG('aurora_manual_backfill', schedule=None, start_date=START,
         catchup=False, max_active_runs=1, default_args=DEFAULTS,
         params={**manual_params(), 'entity': Param('all', type='string', enum=['all'] + SALES)},
         tags=['portfolio', 'backfill', 'synthetic']) as manual_backfill:
    branch = BranchPythonOperator(task_id='choose_entities', python_callable=choose_entities)
    loads = [PythonOperator(task_id=f'load_{entity}', python_callable=manual_entity,
                            op_kwargs={'entity': entity}) for entity in SALES]
    done = EmptyOperator(task_id='done', trigger_rule='none_failed_min_one_success')
    branch >> loads >> done

with DAG('aurora_orders_only', schedule=None, start_date=START,
         catchup=False, max_active_runs=1, default_args=DEFAULTS,
         params=manual_params(), tags=['portfolio', 'orders', 'synthetic']) as orders_only:
    PythonOperator(task_id='load_orders', python_callable=manual_entity, op_kwargs={'entity': 'orders'})

with DAG('aurora_daily_shifts', schedule='0 18 * * *', start_date=START,
         catchup=False, max_active_runs=1, default_args=DEFAULTS,
         params={'dry_run': Param(False, type='boolean')},
         tags=['portfolio', 'shifts', 'synthetic']) as daily_shifts:
    PythonOperator(task_id='load_completed_shifts', python_callable=rolling_shifts)
