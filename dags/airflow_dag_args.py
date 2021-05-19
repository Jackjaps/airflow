#import libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain, cross_downstream

#Setup Dag Arguments 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}

dag = DAG('tutorial', catchup=False, default_args=default_args)

with DAG(name='first dag',schedule_interval='@daily', catchup = True) as dag: 

     bash_task = BashOperator(
        task_id="bash_command",
        bash_command="echo $TODAY",
        env={"TODAY": "2020-05-21"},
        sla=timedelta(hours=2),
    )
    def print_random_number(number):
        seed(number)
        print(random())

    python_task = PythonOperator(
        task_id="python_function", python_callable=print_random_number, op_args=[1],
    )