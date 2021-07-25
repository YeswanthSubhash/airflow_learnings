from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator



def send_arg(**kwargs):
    kwargs['ti'].xcom_push(key='value1',value=10)

def recive_and_sum(**kwargs):
    arg1 = kwargs['ti'].xcom_pull(key='value1',task_ids=['xcom_push'])
    arg2 = 20
    print(arg1[0]+arg2)


default_args = {
    'owner': 'Yeswanth',
    'start_date': days_ago(1),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context':True
}


with DAG('xcom_dag',
         description='Xcom DAG',
         schedule_interval='*/5 * * * *',
         default_args=default_args,
         catchup=False

) as dag:
    dummy_task_start = DummyOperator(
        task_id='Start',
        retries=3
    )

    xcom_push_func = PythonOperator(
        task_id='xcom_push',
        python_callable=send_arg,

    )
    xcom_pull_func = PythonOperator(
        task_id='xcom_pull',
        python_callable=recive_and_sum,

    )

    dummy_task_end = DummyOperator(
        task_id='End',
        retries=3
    )

dummy_task_start >> xcom_push_func >> xcom_pull_func >> dummy_task_end