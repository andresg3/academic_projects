from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from operators.process_song_file import ProcessSongOperator
from operators.process_log_file import ProcessLogOperator
from operators.load_songplays_fact_table import LoadFactTable

from time import sleep
from datetime import datetime, timedelta
import sys
# Insert the scripts directory at the front of the path. Required when using aiflow docker
sys.path.insert(0, '/usr/local/airflow/scripts') 
import ddl_handler

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['airflow@gmail.com'],
    'email_on_failure': True
}

with DAG('postgres_dag', default_args=default_args, description='Postgres DAG', schedule_interval='@once', catchup=False) as dag:
    begin  = DummyOperator(task_id='begin')
    end  = DummyOperator(task_id='end')
    drop_tables = PythonOperator(task_id='drop_tables', python_callable=ddl_handler.drop_tables)
    create_tables = PythonOperator(task_id='create_tables', python_callable=ddl_handler.create_tables)
    process_song_file = ProcessSongOperator(task_id='process_json_song_files', file_path='/usr/local/airflow/data/song_data')
    process_log_file = ProcessLogOperator(task_id='process_json_log_files', file_path='/usr/local/airflow/data/log_data')
    load_songplays_table = LoadFactTable(task_id='load_songplays_fact_table', file_path='/usr/local/airflow/data/log_data')


    begin >> drop_tables >> create_tables >> process_song_file
    create_tables >> process_log_file
    process_song_file >> load_songplays_table
    process_log_file >> load_songplays_table
    load_songplays_table >> end