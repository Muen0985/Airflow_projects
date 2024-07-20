from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime,timedelta

default_args={
    'owner':'Airflow',
    'start_date':datetime.now(),
    'retries':1,
    'retry_delay':timedelta(seconds=5)
}

current_date=datetime.now().date().strftime("%Y-%m-%d")


def push_function(**kwargs):
    message='Today is '+current_date+'from push function'
    ti=kwargs['ti']
    ti.xcom_push(key='mess',value=message)

def pull_function(**kwargs):
    ti=kwargs['ti']
    pull_message=ti.xcom_pull(key="mess",task_ids='push_info')
    print(f"putlled_message: {pull_message}")

with DAG('xcomdag',default_args=default_args,schedule_interval='@daily',catchup=False) as dag:
    t1=BashOperator(task_id='nowdate',bash_command="echo hello airflow world~")
    t2=PythonOperator(task_id='push_info',python_callable=push_function,provide_context=True)
    t3=PythonOperator(task_id='get_info',python_callable=pull_function,provide_context=True)
    t4=BashOperator(task_id='tt1',bash_command='echo print for tt1')
    t5=BashOperator(task_id='tt2',bash_command='echo {{var.value.hello}}')
    t6=DummyOperator(task_id='combine_tt')
    t7=BashOperator(task_id='tt3',bash_command='echo {{var.value.hello}}')
    t8=BashOperator(task_id='tt4',bash_command='echo print for tt4')

    t1 >> t2 >> t3 >> [t4,t5] >> t6 >>[t7,t8]