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
PATH_FILE=Variable.get("UploadPathFile")
list_of_files= Variable.get("fileList",deserialize_json = True)
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

    def pull_function(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='getList')
    print(ls)
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
    task_name= "rowsInFile_"+file
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids=task_name)
    number = ls.split(" ")
    logging.info("Value of the result: {0} type of the result: {1} and the number is:{2}".format(ls,type(ls),int(number[0])-1))
    #ti.xcom_push(key='file', value=)
    return int(number[0])-1

with DAG(
    'LoadInformation',
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
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=['GCP'],
) as dag:

    start =   DummyOperator(task_id="Start")
    endgame = DummyOperator(task_id="End")

    create_common_table = PostgresOperator(
        task_id="create_common_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""CREATE TABLE IF NOT EXISTS cryptoDataCommon (
            date date NOT NULL,
            open decimal,
            low decimal,
            high decimal,
            close decimal,
            volume decimal,
            crypto varchar(50)
            );""",
    )

    with TaskGroup(group_id='dynamic_tasks_group',prefix_group_id=False) as dynamic_tasks_group:
        if list_of_files:
            logging.info("List Of Files  {0}".format(list_of_files))
            for i,fileN in enumerate(list_of_files):
                #Dimanic Variables
                file_name=fileN[0:3]
                
                # PostgresOperator - Create Staging Tables
                create_table = PostgresOperator(
                    task_id="create_table_{}".format(file_name),
                    postgres_conn_id=POSTGRES_CONN_ID,
                    sql="""CREATE TABLE IF NOT EXISTS cryptoData_{{ params.cripto }}_stg (
                        date date NOT NULL,
                        open VARCHAR(20),
                        low VARCHAR(20),
                        high VARCHAR(20),
                        close VARCHAR(20),
                        volume VARCHAR(20)
                        );""",
                    params={'cripto': file_name }
                )

                # PostgresOperator - Truncate Staging Tables
                truncate_table = PostgresOperator(
                    task_id="truncate_table_{}".format(file_name),
                    postgres_conn_id=POSTGRES_CONN_ID,
                    sql="""truncate table cryptoData_{{ params.cripto }}_stg;""",
                    params={'cripto': file_name }
                )

                # BashOperator - Data Validation File Rows
                dv_file_rows = BashOperator(
                    task_id="rowsInFile_{}".format(file_name),
                    bash_command="echo $(wc -l /opt/airflow/data/cryptoPrices/$file)",
                    env={"file":fileN}
                    )
                # BashOperator - Extract value of Row Count into Airflow XCOM 
                extractRowCount = PythonOperator(
                    task_id="validations{}".format(file_name),
                    python_callable=valid,
                    provide_context=True,
                    op_kwargs= {"file":file_name}
                )

                # PythonOperator - Upload files to Postgres Database
                up_load_files_to_db = PythonOperator(
                    task_id='upload_files_toDB_{}'.format(file_name),
                    python_callable=PosgresBulkLoadCSV,
                    op_kwargs={"file_path": "/opt/airflow/data/cryptoPrices/",
                    "filen": fileN,
                    "table": "cryptoData_{0}_stg".format(file_name)
                    },
                    provide_context=True
                )

                # PostgresOperator - Move and Clean into Common Table
                move_clean_data = PostgresOperator(
                    task_id="move_clean_data_{}".format(file_name),
                    postgres_conn_id=POSTGRES_CONN_ID,
                    sql="""
                        insert into {{ params.targetTable }} select 
                        date,
                        CAST(REPLACE(open,',','') AS DECIMAL),
                        CAST(REPLACE(low, ',', '') AS DECIMAL),
                        CAST(REPLACE(high, ',', '') AS DECIMAL),
                        CAST(REPLACE(close, ',', '') AS DECIMAL),
                        CAST(REPLACE(volume, ',', '') AS DECIMAL),
                        '{{ params.cripto }}'
                        from cryptoData_{{ params.cripto }}_stg;
                        """,
                    params={'cripto': file_name,'targetTable':"cryptoDataCommon" }
                )

                #SQLValueCheckOperator - Validate XCOM Row count from File vs Common Table in DB
                value_check = SQLValueCheckOperator(
                    task_id="check_row_count_{}".format(file_name),
                    conn_id=POSTGRES_CONN_ID,
                    database='oltp',
                    sql="SELECT COUNT(*) FROM cryptoDataCommon where crypto = '{0}';".format(file_name),
                    pass_value= f"{{{{ task_instance.xcom_pull(task_ids='validations{file_name}') }}}}",
                    email_on_failure = "jacobo.mleguizamo@gmail.com"
                )
                create_table >> truncate_table >> dv_file_rows >> extractRowCount >> up_load_files_to_db >> move_clean_data >> value_check
    
    upload_data_to_gcs = PostgresToGCSOperator(
        task_id="upload_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT * FROM cryptoDataCommon", 
        bucket=GCS_BUCKET, 
        filename="cryptocurrency/cryptoDataCommon", 
        use_server_side_cursor=True,
        gzip=False
    )
    start >> create_common_table >> dynamic_tasks_group >> upload_data_to_gcs >> endgame

