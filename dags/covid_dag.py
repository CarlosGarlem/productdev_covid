import os

from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger
import pandas as pd

logger = get_logger()

COLUMNS = {
    "id": "id",
    "name": "name"
}

FILE_CONNECTION_NAME = 'fs_data'
CONNECTION_DB_NAME = 'covid_db'

def etl_process(**kwargs):
    logger.info(kwargs["execution_date"])
    file_path = FSHook(FILE_CONNECTION_NAME).get_path()
    filename = 'data.csv'
    mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    full_path = f'{file_path}/{filename}'
    df = (pd.read_csv(full_path, encoding = "UTF-8", usecols=COLUMNS.keys())
          .rename(columns=COLUMNS)
          )

    with mysql_connection.begin() as connection:
        connection.execute("DELETE FROM dm_covid.covid WHERE 1=1")
        df.to_sql('covid', con=connection, schema='dm_covid', if_exists='append', index=False)

    os.remove(full_path)

    logger.info(f"Rows inserted {len(df.index)}")





dag = DAG('data_ingestion_dag', description='Dag to Ingest Data',
          default_args={
              'owner': 'edgar.saban',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(2)
          },
          schedule_interval='0 1 * * *',
          catchup=False)

sensor = FileSensor(task_id="file_sensor_task",
                    dag=dag,
                    filepath='data.csv',
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

etl = PythonOperator(task_id="data_etl",
                     provide_context=True,
                     python_callable=etl_process,
                     dag=dag
                     )

sensor >> etl
