from airflow import DAG
from datetime import datetime ,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

default_args={
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'start_day':datetime.now()
}

def transfer_function(ds, **kwargs):
        query="SELECT * FROM source_city_table"

        # because of using two tables, two hooks and conns are required

        #source hook
        source_hook=PostgresHook(postgres_conn_id='postgres_conn',schema='airflow')
        source_conn=source_hook.get_conn()
        #destination hook
        destination_hook=PostgresHook(postgres_conn_id='postgres_conn',schema='airflow')
        destination_conn=destination_hook.get_conn()

        source_cursor= source_conn.cursor()
        destination_cursor= destination_conn.cursor()

        source_cursor.execute(query)
        records=source_cursor.fetchall()

        if records:
            execute_values(destination_cursor,"INSERT INTO target_city_tables %s",records)
            destination_conn.commit()

        source_cursor.close()
        destination_cursor.close()
        source_conn.close()
        destination_conn.close()
        print("data transferred successfully!")

with DAG('postgres_hooks_demo',default_args=default_args,schedule_interval='@daily') as dag:
    t1=PythonOperator(task_id='transferdata',python_callable=transfer_function,provide_context=True)


# create table source_city_table(city_name varchar (50), city_code varchar (20));
# insert into source_city_table (city_name, city_code) values('New York', 'ny'), ('Los Angeles', 'la'), ('Chicago', 'cg'), ('Houston', 'ht');
# create table target_city_table as (select * from source_city_table) with no data;
# select * from target_city_table;

# execute 是用于执行任意 SQL 查询的通用方法，包括 SELECT、INSERT、UPDATE、DELETE 等。
# 它适用于单次执行一个 SQL 语句。
#　execute_values 是一个专门用于批量插入数据的辅助函数。它可以将多个值一次性插入到数据库中，
# 减少数据库通信次数，提高插入操作的效率。