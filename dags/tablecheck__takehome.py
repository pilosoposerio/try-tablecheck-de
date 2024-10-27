import logging

from pathlib import Path

import duckdb
import pendulum

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain




@dag(
    dag_id=Path(__file__).stem,
    schedule="@hourly", 
    start_date=pendulum.datetime(2024, 10, 26),
    catchup=False,
    tags=['tablecheck']
)
def takehome_dag():
    
    wait_files = FileSensor(
        task_id='wait_files',
        filepath='/data/tablecheck/raw/*.csv',
        poke_interval=60,
        timeout=600,
        soft_fail=True
    )

    @task()
    def load_file_to_warehouse():
        """
        At this step, we load the raw data files to the data warehouse. This can be replaced
        with one or more built-in or custom operators depending on where the data is coming from
        and where is it going to e.g., S3ToRedshiftOperator, S3ToPostgresOperator, etc. But here,
        I will be loading data from local filesystem to "local" DuckDB data warehouse.
        """
        logger = logging.getLogger('airflow.task')
        try:
            with duckdb.connect('/data/warehouse/tablecheck.duckdb') as con:
                con.sql(
                    """
                    CREATE OR REPLACE TABLE raw_data AS
                    SELECT 
                        *
                        , UUID()::TEXT AS  transaction_id
                        , CURRENT_TIMESTAMP AS loaded_at
                    FROM read_csv('/data/tablecheck/raw/*.csv', filename=true, union_by_name=true)
                    """
                )
        except Exception as e:
            logger.exception("Failed to load files to warehouse.")
            for file in Path('/data/tablecheck/raw').glob('*.csv'):
                file.rename('/data/tablecheck/error/' + file.name)
    
    @task()
    def archive_files():
        """
        At this step, we archive the "ingested" raw data files to a different location to clean up the "staging area".
        """
        for file in Path('/data/tablecheck/raw').glob('*.csv'):
            file.rename('/data/tablecheck/archive/' + file.name)

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --select tablecheck.* --target prod',
        env={'DBT_PROFILES_DIR': '/tablecheck_dbt', 'PATH': '$PATH:/usr/local/airflow/.local/bin'},
        cwd='/tablecheck_dbt',
    )

    chain(wait_files, load_file_to_warehouse(), archive_files(), run_dbt_models)

this_dag = takehome_dag()