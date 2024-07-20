import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

def push_func(**kwargs):
    pushval=5
    ti=kwargs['ti']
    ti.xcom_push(key='num',value=pushval)
def branch_func(**kwargs):
    ti=kwargs['ti']
    pulled_value=ti.xcom_pull(key='num',task_ids='push_task')
    if pulled_value %2 == 0:
        return 'even_task'
    else:
        return 'odd_task'
    
with DAG(dag_id='branching', default_args=args, schedule_interval="@daily") as dag:
    push_task=PythonOperator(task_id='push_task',python_callable=push_func,provide_context=True)

    branch_task=BranchPythonOperator(task_id='branch_task',python_callable=branch_func,provide_context=True)

    even_task=BashOperator(task_id='even',bash_command='echo "got an even value."')
    odd_task=BashOperator(task_id='odd',bash_command='echo "got an odd value."')

    push_task >> branch_task >> [even_task,odd_task]
