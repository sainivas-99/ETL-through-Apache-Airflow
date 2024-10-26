from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

args = {
        'retries' : 3,
        'owner' : 'airflow',
        'retry_delay' : timedelta(minutes=1)
}

log_file = open('./marks_update_log.txt','a')

def logging():
    output = f'{datetime.now()} : End of airflow task'
    log_file.write(output)

with DAG(
    dag_id = 'Marks_updates',
    description= 'Used to update the marks in ETL_data table',
    default_args = args,
    schedule = timedelta(minutes=2),
    start_date = datetime(2024, 10, 23, 0, 24),
) as dag:
    t1 = BashOperator(
        task_id = 'update',
        bash_command = 'python3 /Users/sainivasrangaraju/Desktop/ETL/latest_marks.py'
    )

    t2 = PythonOperator(
        task_id = 'logging',
        python_callable =  logging
    )

    t1.set_downstream(t2)