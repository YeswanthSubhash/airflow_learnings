# We'll start by importing the DAG object
from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.dummy import DummyOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago
from datetime import timedelta

# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'Yeswanth',
    'start_date': days_ago(5),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Createion
my_first_dag = DAG(
    dag_id='DummyOperator',
    default_args=default_args,
    description='Our first DAG',
    schedule_interval=timedelta(days=1),
)

# Dummy Operator Commands
start = DummyOperator(
    task_id='Start',
    dag=my_first_dag,
)

end = DummyOperator(
    task_id='End',
    dag=my_first_dag,
)

start >> end

