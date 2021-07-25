from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor




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

def file_check():
    print("File has been found !!")


with DAG('sensor_dag',
         description='Sensor DAG',
         schedule_interval=None,
         default_args=default_args,
         catchup=False
) as dag:

    sensing_task = FileSensor(
        task_id='sensing_task',
        filepath='test.txt',
        fs_conn_id='my_file_system',
        poke_interval=10
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=file_check,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=2)
    )

    sensing_task >> python_task