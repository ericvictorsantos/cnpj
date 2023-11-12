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
# from cnpj.src.bll.load import Load
from cnpj.src.bll.extract import Extract
# from cnpj.src.bll.transform import Transform

config = Config().load_config()

local_or_cloud = config['environment']['local_or_cloud']
time_zone = config['environment']['timezone']
job_name = config['job']['name']

default_args = {
    'days_ago': 1,
    'email': 'eric.santos@n3urons.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': 'None',
    'seconds_retry_delay': 60,
    'minutes_dag_run_timeout': 60,
    'tags': 'data/ia'
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
args = Variable.get(job_name, default_var=None, deserialize_json=True)

# set variable
if args is None:
    args = default_args
    Variable.set(key=job_name, value=args, serialize_json=True)

args['owner'] = 'eric.santos'
args['email'] = [email.strip() for email in args.get('email').split(',')]
args['tags'] = [tag.strip() for tag in args.get('tags').split(',')]
args['retry_delay'] = timedelta(seconds=args['seconds_retry_delay'])
schedule_interval = None if local_or_cloud == 'local' else args['schedule_interval']
schedule_interval = None if schedule_interval == 'None' else schedule_interval
args['schedule_interval'] = schedule_interval

dag_args = {
    'dag_id': job_name,
    'start_date': days_ago(args.pop('days_ago')),
    'schedule_interval': args.pop('schedule_interval'),
    'concurrency': 1,
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=args.pop('minutes_dag_run_timeout')),
    'default_args': args
}

with DAG(**dag_args) as dag:
    extract = Extract()
    # transform = Transform()
    # load = Load()

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract.run
    )
    # transform_task = PythonOperator(
    #     task_id='transform',
    #     python_callable=transform.run,
    #     op_args=['{{ run_id }}']
    # )
    # load_task = PythonOperator(
    #     task_id='load',
    #     python_callable=load.run,
    #     op_args=['{{ run_id }}']
    # )

    # extract_task >> transform_task >> load_task
