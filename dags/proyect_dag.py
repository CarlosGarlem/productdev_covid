# Airflow imports
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator  import DummyOperator
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
from airflow import DAG

# Utilities imports
from etl_code.landing_files import CovidFile
from etl_code.dimensions import RegionDimension, DateDimension
from etl_code.fact_consolidation import FactCovid

# Libraries imports
from structlog import get_logger
import pandas as pd
import os

FILE_CONNECTION_NAME = 'fs_data'
CONNECTION_DB_NAME = 'covid_db'
FILE_NAMES = {'confirmed': 'time_series_covid19_confirmed_global.csv',
            'deaths': 'time_series_covid19_deaths_global.csv',
            'recovered': 'time_series_covid19_recovered_global.csv'}

dag = DAG('covid_cases_dag', 
        description='Process to ingest confirmed cases.',
        default_args={
            'owner': 'ricardo.mendoza',
            'depends_on_past': False,
            'max_active_runs': 1,
            'start_date': days_ago(1)
        },
        schedule_interval='@once',
        catchup=False)

# Stage Operators 
start_stage = DummyOperator(task_id='start_stage', dag=dag)

ingestions_stage = DummyOperator(task_id='ingestions_stage', dag=dag)

dimensions_stage = DummyOperator(task_id='dimensions_stage', dag=dag)

consolidation_stage = DummyOperator(task_id='consolidation_stage', dag=dag)

end = DummyOperator(task_id='end', dag=dag)

# Ingestion process
def ingestion_process(**kwargs) -> None:
    engine = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME)\
        .get_sqlalchemy_engine()
    etl = CovidFile(file_path=FSHook(FILE_CONNECTION_NAME).get_path(),
                    file_names=FILE_NAMES,
                    db_con=engine)
    etl.run()

# Dimensions builders
def build_region_dimension(**kwargs) -> None:
    engine = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME)\
        .get_sqlalchemy_engine()
    builder = RegionDimension(db_con=engine)
    builder.build_dimension()

def build_date_dimension(**kwargs) -> None:
    engine = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME)\
        .get_sqlalchemy_engine()
    builder = DateDimension(db_con=engine)
    builder.build_dimension()

# Fact builder
def build_fact_covid(**kwargs) -> None:
    engine = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME)\
        .get_sqlalchemy_engine()
    builder = FactCovid(db_con=engine)
    builder.build_fact_table()

# Sensors
sensor_confirmed = FileSensor(task_id="file_sensor_task_confirmed",
                    dag=dag,
                    filepath=FILE_NAMES['confirmed'],
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

sensor_deaths = FileSensor(task_id="file_sensor_task_deaths",
                    dag=dag,
                    filepath=FILE_NAMES['deaths'],
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

sensor_recovered = FileSensor(task_id="file_sensor_task_recovered",
                    dag=dag,
                    filepath=FILE_NAMES['deaths'],
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

# Ingestion Operator
ingestion = PythonOperator(task_id="ingestion",
                            dag=dag,
                            python_callable=ingestion_process,
                            provide_context=True)


# Dimensions and Fact Operator
d_region = PythonOperator(task_id="d_region",
                            dag=dag,
                            python_callable=build_region_dimension,
                            provide_context=True)

d_date = PythonOperator(task_id="d_date",
                            dag=dag,
                            python_callable=build_date_dimension,
                            provide_context=True)

f_covid = PythonOperator(task_id="f_covid",
                            dag=dag,
                            python_callable=build_fact_covid,
                            provide_context=True)

start_stage >> [sensor_confirmed, sensor_deaths, sensor_recovered] >> ingestions_stage
ingestions_stage >> ingestion >> dimensions_stage
dimensions_stage >> [d_region, d_date] >> consolidation_stage
consolidation_stage >> f_covid >> end
