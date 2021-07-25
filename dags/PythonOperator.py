from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from apps.mydag.functions import function_from_outside_dags
from apps.mydag.without_pythonop import process
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
def my_func():
    print('Hello from my_func')

def my_second_func():
    return 'This is my second function'

def passing_parameters(filename,path):
    print(filename)
    print(path)
    return filename
def passing_positional_parameters(filename,path):
    print(filename)
    print(path)
    return filename
def value_from_variable(filename,path):
    print(filename)
    print(path)
    return filename
def value_from_json(filename,path):
    print(filename)
    print(path)
    return filename

with DAG('python_dag',
         description='Python DAG',
         schedule_interval='*/5 * * * *',
         start_date=datetime(2018, 11, 1),
         catchup=False

) as dag:


    dummy_task = DummyOperator(
            task_id='dummy_task',
            retries=3
         )
    python_task = PythonOperator(
            task_id='python_task',
            python_callable=my_func
    )
    python_second_task = PythonOperator(
        task_id='python_second_task',
        python_callable=my_second_func
    )
    # python_third_task = PythonOperator(
    #     task_id='Passing Arguments',
    #     python_callable=passing_parameters,
    #     op_kwargs = {'filename': "data.txt",
    #                'path': "/usr/path/file"
    #                }
    # )
    # python_fourth_task = PythonOperator(
    #     task_id='Passing Positional Arguments',
    #     python_callable=passing_positional_parameters,
    #     op_args=['data.txt','/usr/path/file']
    #
    # )
    python_fifth_task = PythonOperator(
        task_id='python_variables_value',
        python_callable=value_from_variable,
        op_kwargs = {'filename': '{{var.value.filename}}',
                        'path': '{{var.value.path}}'
                    }
    )
    python_sixth_task = PythonOperator(
        task_id='python_variables_json',
        python_callable=value_from_json,
        op_kwargs=Variable.get('my_dag_settings',deserialize_json=True)
    )
    #set up PYTHONPATH as AIRFLOW_HOME
    python_seventh_task = PythonOperator(
        task_id='python_function_from_apps',
        python_callable=function_from_outside_dags,
        op_kwargs=Variable.get('my_dag_settings', deserialize_json=True)
    )

    #calling function without dag
    process(Variable.get('path'))







#dummy_task >> python_task

#dummy_task >> python_task >> python_second_task
dummy_task >> python_task >> python_second_task >> python_fifth_task>>python_sixth_task>>python_seventh_task

