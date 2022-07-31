from datetime import datetime, timedelta
from textwrap import dedent
import logging
import sys,os


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.sql import SQLValueCheckOperator


from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


#Env Variables 
POSTGRES_CONN_ID=Variable.get("postgress_connID")
PATH_FILE=Variable.get("collatzPath")
list_of_files= Variable.get("collatzFiles",deserialize_json = True)
GCS_BUCKET="jacobs_bucket"
GCS_CONN_ID=Variable.get("gcs_connID")



#Environment functions
"""
def on_failure_callback(context):
    # with mail:
    error_mail = EmailOperator(
        task_id='error_mail',
        to='user@example.com',
        subject='Fail',
        html_content='a task failed',
        mime_charset='utf-8')
    error_mail.execute({})  # no need to return, just execute

    # with slack:
    error_message = SlackAPIPostOperator(
        task_id='error_message',
        token=getSlackToken(),
        text='a task failed',
        channel=SLACK_CHANNEL,
        username=SLACK_USER)
    error_message.execute({})
"""


class NewHookPostgress(PostgresHook):
    def bulk_load(self, table: str, tmp_file: str) -> None:
        self.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER QUOTE '\"' ", tmp_file)

def PosgresBulkLoadCSV(file_path,filen,table):
    pg_hook=NewHookPostgress(postgres_conn_id=POSTGRES_CONN_ID)
    logging.info("Importing Files query to file")
    namec_file = file_path + filen
    logging.info("Name Of File {0}".format(namec_file))
    pg_hook.bulk_load(table,namec_file)

def getFileList(path):
    list_of_files=[]
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            list_of_files.append(file)
    logging.info("List Of Files first {0}".format(list_of_files))

def validation_function():
    from data_validation import data_validation
    from data_validation.result_handlers import bigquery as bqhandler
    # BigQuery Specific Connection Config
    GROUPED_CONFIG_COUNT_VALID = {
        # Configuration Required for All Data Sources
        "source_type": "Postgres",
        # Connection Details
        "host": "oltp-db",
        "port":5432,
        "user": "root",
        "password": "root",
        "database":"oltp"
    }
    handler = bqhandler.BigQueryResultHandler.get_handler_for_project(project)
    validator = data_validation.DataValidation(
        GROUPED_CONFIG_COUNT_VALID, verbose=True, result_handler=handler
    )
    validator.execute()



def valid(file,**kwargs):
    task_name= "Rows_In_File_"+file
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids=task_name)
    number = ls.split(" ")
    logging.info("Value of the result: {0} type of the result: {1} and the number is:{2}".format(ls,type(ls),int(number[0])-1))
    #ti.xcom_push(key='file', value=)
    return int(number[0])-1

with DAG(
    'Load_Collatz_Data',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        #'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='upload big data into gcp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=['GCP'],
) as dag:

    start =   DummyOperator(task_id="Start")
    endgame = DummyOperator(task_id="End")

    # PostgresOperator - Create Staging Tables
    create_table = PostgresOperator(
        task_id="create_table_collatz_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""CREATE TABLE IF NOT EXISTS CollatzResults (
                    number int NOT NULL,
                    iterations int,
                    maxValue int,
                    starttime NUMERIC(10,6),
                    endtime NUMERIC(10,6),
                    differ NUMERIC(10,16)
                    );"""
    )
    truncate_table = PostgresOperator(
        task_id="truncate_table_collatz_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="TRUNCATE table CollatzResults;"
    )

    with TaskGroup(group_id='dynamic_tasks_group',prefix_group_id=False) as dynamic_tasks_group:
        if list_of_files:
            logging.info("Files to process:  {0}".format(list_of_files))
            for i,fileN in enumerate(list_of_files):
                #Dimanic Variables
                file_name=fileN
                # PythonOperator - Upload files to Postgres Database
                up_load_files_to_db = PythonOperator(
                    task_id='upload_files_toDB_{}'.format(file_name),
                    python_callable=PosgresBulkLoadCSV,
                    op_kwargs={"file_path": PATH_FILE,
                    "filen": fileN,
                    "table": "CollatzResults"
                    },
                    provide_context=True
                )
                
            up_load_files_to_db
    
    upload_data_to_gcs = PostgresToGCSOperator(
        task_id="upload_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT * FROM CollatzResults", 
        bucket=GCS_BUCKET, 
        filename="CollatzResults/collatzData", 
        use_server_side_cursor=True,
        gzip=False
    )
    start  >> create_table >> truncate_table >> dynamic_tasks_group >> upload_data_to_gcs >> endgame

