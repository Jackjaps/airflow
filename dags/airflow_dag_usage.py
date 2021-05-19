#import libraries
from airflow import DAG

#Option1
dag = DAG(name='first dag',schedule_interval='0 * * *', catchup = True)
op = BashOperator(dag=dag)


#Option2
with DAG(name='first dag',schedule_interval='@daily', catchup = True) as dag: 
    op = BashOperator()
#@daily = '0 0 * * *'



