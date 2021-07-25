from airflow.decorators import task

@task(task_id="task_a")
def process(params):
    print(params)