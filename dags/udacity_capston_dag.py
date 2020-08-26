from datetime import datetime, timedelta
import os
import json
from airflow import DAG
import configparser
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from helpers.sql_statements import sqlCreateAndQuery as sq
from helpers.create_tables import sqlCreateTables as cr
from helpers.sql_load_tables import sqlLoadingQuery as InsertQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                              LoadDimensionOperator, DataQualityOperator)

aws_credentials_id = 'aws_credentials'
redshift_credentials_id = 'redshift' 
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 18),
}

dag = DAG('udacity_capston_dag', 
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@once',
          start_date = datetime(2020, 7, 18)
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
start_create_AWS_operator = DummyOperator(task_id='start_create_AWS_operator_task',  dag=dag)

####### start  creating redshift cluster  process ########


create_AWS_resources_operator = BashOperator(
    task_id='create_AWS_resources_task',
    bash_command='python /home/workspace/airflow/AWS/Run_Server.py',
    dag=dag)
####### end  creating redshift cluster  process ########

####### start creating staging table process ########

start_create_tables_operator = DummyOperator(task_id='creating_staging_table_process' , dag=dag)

create_table_flight = PostgresOperator(
    task_id="stage_flights_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_FLIGHTS_STAGING_TABLE
)

create_table_weather = PostgresOperator(
    task_id="staging_weather_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_WEATHER_STAGING_TABLE
)

create_table_airport = PostgresOperator(
    task_id="staging_airport_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_AIRPORT_STAGING_TABLE
)

create_table_carriers = PostgresOperator(
    task_id="staging_carriers_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_CARRIERS_STAGING_TABLE
)
 ####### end creating staging table process ########

start_satge_operator= DummyOperator(task_id='start_satge_operator_task',  dag=dag)
######### start staging process ######
create_table_staging_flights = StageToRedshiftOperator(
    task_id="flights_staging",
    table = "flights_staging_table",
    credentials = aws_credentials_id,
    redshift = redshift_credentials_id,
    s3 = 'flights-dend/flight_dataset/flight.csv', 
    dag =dag
)

create_table_staging_weather = StageToRedshiftOperator(
    task_id="weather_staging",
    table = "weather_staging_table",
    credentials = aws_credentials_id,
    redshift = redshift_credentials_id,
    s3 = 'flights-dend/flights-dend-supplementary/GlobalLandTemperaturesByCity.csv', 
    dag =dag
)

create_table_staging_airport = StageToRedshiftOperator(
    task_id="airport_staging",
    table = "airport_staging_table",
    credentials = aws_credentials_id,
    redshift = redshift_credentials_id,
    s3 = 'flights-dend/flights-dend-supplementary/airports.csv', 
    dag =dag
)

create_table_staging_carriers = StageToRedshiftOperator(
    task_id="carriers_staging",
    table = "carriers_staging_table",
    credentials = aws_credentials_id,
    redshift = redshift_credentials_id,
    s3 = 'flights-dend/flights-dend-supplementary/carriers.csv', 
    dag =dag
)

 ####### end staging process ########
                                             
####### start creating fact and dimintion process ########
fact_dim_table_operator = DummyOperator(task_id='create_fact_dim_table_operator',  dag=dag)

create_dim_Airport = PostgresOperator(
    task_id="create_dim_Airport",
    dag=dag,
    postgres_conn_id="redshift",
    sql=cr.create_dim_Airport
)
create_dim_carriers = PostgresOperator(
    task_id="create_dim_carriers",
    dag=dag,
    postgres_conn_id="redshift",
    sql=cr.create_dim_carriers
)
create_dim_weather = PostgresOperator(
    task_id="create_dim_weather",
    dag=dag,
    postgres_conn_id="redshift",
    sql=cr.create_dim_weather
)
create_fact_flights = PostgresOperator(
    task_id="create_fact_flights",
    dag=dag,
    postgres_conn_id="redshift",
    sql=cr.create_fact_flights
)
 
####### end creating fact and dimintion process ########
####### load data from staging into fact and dimintion process ########
Load_data_Operator = DummyOperator(task_id='Load_data_Operator',  dag=dag)

load_dim_Airport_table = LoadDimensionOperator(
    task_id='load_dim_Airport_table',
    table = "dim_airports",
    redshift= 'redshift',
    column = "(iata, airport, city, country, wether)",
    clear_data = 'YES',
    sql = InsertQueries.AIRPORT_DIM_INSERT,
    dag = dag
)

load_dim_carriers_table = LoadDimensionOperator(
    task_id='load_dim_carriers_table',
    table = "dim_carriers",
    redshift= 'redshift',
    column = "(code, Description, tail_number)",
    clear_data = 'YES',
    sql = InsertQueries.CARRIERS_DIM_INSERT,
    dag = dag
)

load_dim_weather_table = LoadDimensionOperator(
    task_id='load_dim_weather_table',
    table = "dim_weather",
    redshift= 'redshift',
    column = "(date, AverageTemperature, AverageTemperatureUncertainty)",
    clear_data = 'YES',
    sql = InsertQueries.WEATHER_DIM_INSERT,
    dag = dag
)

load_fact_flights_table = LoadFactOperator(
    task_id='load_fact_flights_table',
    table = "fact_flights", 
    redshift = 'redshift',
    column = "(flight_id, airline, DepTime, ArrTime, FlightNum, UniqueCarrier, orgin, Dest, dt)",
    clear_data = 'YES',
    sql = InsertQueries.FLIGHT_FACT_INSERT,
    dag = dag
)
##### check qulity #######
start_data_quality_check_operator = DummyOperator(task_id='start_data_quality_check',  dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',  
    redshift= 'redshift',
    dag=dag
)


end_operator = DummyOperator(task_id='end_operator',  dag=dag)


start_operator >> start_create_AWS_operator
start_create_AWS_operator >> create_AWS_resources_operator 
create_AWS_resources_operator >> start_create_tables_operator
start_create_tables_operator >> [create_table_flight, create_table_weather, create_table_airport, create_table_carriers]
[create_table_flight, create_table_weather, create_table_airport, create_table_carriers]  >>    start_satge_operator
start_satge_operator >> [create_table_staging_flights, create_table_staging_weather, create_table_staging_airport, create_table_staging_carriers]
[create_table_staging_flights, create_table_staging_weather, create_table_staging_airport, create_table_staging_carriers]  >> fact_dim_table_operator
fact_dim_table_operator >>  [create_dim_Airport, create_dim_carriers, create_dim_weather, create_fact_flights]
[create_dim_Airport, create_dim_carriers, create_dim_weather, create_fact_flights]  >> Load_data_Operator
Load_data_Operator >> [load_dim_Airport_table, load_dim_carriers_table, load_dim_weather_table, load_fact_flights_table]
[load_dim_Airport_table, load_dim_carriers_table, load_dim_weather_table, load_fact_flights_table] >> start_data_quality_check_operator 
start_data_quality_check_operator >> run_quality_checks 
run_quality_checks >> end_operator
                                             