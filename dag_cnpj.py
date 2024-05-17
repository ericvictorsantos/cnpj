# coding: utf-8
""" DAG Configuration Module """

# built-in
from time import tzset
from os import environ as os_environ
from datetime import datetime, timedelta

# installed
from airflow import DAG
from pendulum import timezone
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# custom
from cnpj.config import Config
from cnpj.src.bll.main import Main

main = Main()
config = Config().load_config()

local_or_cloud = config['environment']['local_or_cloud']
time_zone = config['environment']['timezone']
job_name = config['job']['name']

job_default_params = {
    'args': {
        'days_ago': 1,
        'email': 'ericvictorsantos@outlook.com',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'schedule_interval': 'None',
        'seconds_retry_delay': 60,
        'minutes_dag_run_timeout': 60,
        'tags': 'data/ia'
    }
}

local_tz = timezone(time_zone)
os_environ['TZ'] = time_zone
tzset()


def days_ago(days):
    """
    Get a datetime object representing minutes ago.

    Parameters
    ----------
    days : int
        Days ago.

    Returns
    -------
    date_time : datetime
        Date time representing days ago.
    """

    date_time = datetime.now(tz=local_tz)
    date_time = date_time.replace(hour=0, minute=0, second=0, microsecond=0)

    date_time = date_time - timedelta(days=days)

    return date_time


# get variable
job_params = Variable.get(key=job_name, default_var={}, deserialize_json=True)
job_params_ = job_params.copy()
if 'args' not in job_params.keys():
    job_params = job_default_params

if 'params' not in job_params.keys():
    job_params['params'] = config['params']

# set variable
if job_params_ != job_params:
    Variable.set(key=job_name, value=job_params, serialize_json=True)

dag_params = job_params.pop('args')
dag_params['owner'] = 'eric.santos'
dag_params['email'] = [email.strip() for email in dag_params.get('email').split(',')]
dag_params['retry_delay'] = timedelta(seconds=dag_params['seconds_retry_delay'])
schedule_interval = None if local_or_cloud == 'local' else dag_params['schedule_interval']
schedule_interval = None if schedule_interval == 'None' else schedule_interval
dag_params['schedule_interval'] = schedule_interval

dag_args = {
    'dag_id': job_name,
    'start_date': days_ago(dag_params.pop('days_ago')),
    'schedule_interval': dag_params.pop('schedule_interval'),
    'concurrency': 1,
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=dag_params.pop('minutes_dag_run_timeout')),
    'default_args': dag_params
}

with DAG(**dag_args) as dag:
    start_task = PythonOperator(
        task_id='start',
        python_callable=main.run_start,
        op_args=[config]
    )
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=main.run_extract,
        op_args=[config]
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=main.run_transform,
        op_args=[config]
    )
    # load_task = PythonOperator(
    #     task_id='load',
    #     python_callable=main.run_load,
    #     op_args=[config]
    # )
    # end_task = PythonOperator(
    #     task_id='transform',
    #     python_callable=main.run_end,
    #     op_args=[config]
    # )

    start_task >> extract_task >> transform_task
