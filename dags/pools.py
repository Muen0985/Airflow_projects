from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args={
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(seconds=30),
    'start_day':datetime.now()
}

with DAG('pools',default_args=default_args,schedule_interval='@daily') as dag:
    t1=BashOperator('t1',bash_command='sleep 3',pool='pool_1')
    t2=BashOperator('t2',bash_command='sleep 3',pool='pool_1')
    t3=BashOperator('t3',bash_command='sleep 3',pool='pool_2',priority_weight=2)
    t4=BashOperator('t4',bash_command='sleep 3',pool='pool_2')