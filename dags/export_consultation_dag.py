import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import dotenv_values
from includes.query_manager import QueryManager
from includes.postgres_connector import SupabaseQueryExecutor

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
        # Initialize the executor
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
        print(query)
        
        # Execute query
        results = executor.execute(query)
        
        # Print results
        for row in results:
            print(row)
        
        return results
       
    except Exception as e:
        print(f"Error querying Supabase: {str(e)}")
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

# For local testing
if __name__ == "__main__":
    print("Running Supabase query locally...")
    print("-" * 50)
   
    # Set your Supabase credentials here for local testing
    os.environ['SUPABASE_HOST'] = 'your-project.supabase.co'
    os.environ['SUPABASE_DB'] = 'postgres'
    os.environ['SUPABASE_USER'] = 'postgres'
    os.environ['SUPABASE_PASSWORD'] = 'your-password'
    os.environ['SUPABASE_PORT'] = '5432'
   
    # Run the query function
    try:
        results = query_supabase()
        print("-" * 50)
        print(f"Successfully retrieved {len(results)} rows")
    except Exception as e:
        print(f"Failed to query database: {str(e)}")
