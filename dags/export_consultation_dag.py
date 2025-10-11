from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dotenv import dotenv_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from includes.query_manager import QueryManager
from includes.postgres_connector import SupabaseQueryExecutor
import duckdb

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def query_supabase():
    """Query Supabase Postgres database"""
    _env = dotenv_values(dotenv_path=Path('.env'))
    try:
        # Global useful variables
        motherduck_database_name = _env.get('MOTHERDUCK_DATABASE_NAME', 'my_db')
        
        # Initialize the executor for MotherDuck
        motherduck_token = _env.get('MOTHERDUCK_TOKEN', '')
        con = duckdb.connect(f'md:{motherduck_database_name}?motherduck_token={motherduck_token}')
        
        # Initialize the executor for SupaBase
        executor = SupabaseQueryExecutor(
            host=_env.get('SUPABASE_HOST', 'your-project.supabase.co'),
            database=_env.get('SUPABASE_DB', 'postgres'),
            user=_env.get('SUPABASE_USER', 'postgres'),
            password=_env.get('SUPABASE_PASSWORD', 'your-password'),
            port=_env.get('SUPABASE_PORT', '5432')
        )
        
        # Get query from query manager
        query_manager = QueryManager(queries_directory="./dags/queries")
        query = query_manager.get_query_params(file_path="consultation.sql", parameters={})
        
        # Execute query
        results = executor.execute(query)
        df = pd.DataFrame(results)
        con.sql("CREATE OR REPLACE TABLE consultation AS SELECT * FROM df")
        
        return results
    except Exception as e:
        raise

# Create the DAG
dag = DAG(
    'supabase_query_dag',
    default_args=default_args,
    description='Simple DAG to query Supabase database',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['supabase', 'database'],
)

# Define the task
query_task = PythonOperator(
    task_id='query_supabase_task',
    python_callable=query_supabase,
    dag=dag,
)

if __name__ == "__main__":
    try:
        results = query_supabase()
    except Exception as e:
        print(f"Failed to query database: {str(e)}")
