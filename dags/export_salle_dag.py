from pathlib import Path
from datetime import datetime, timedelta, time, date
import pandas as pd
from dotenv import dotenv_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from includes.query_manager import QueryManager
from includes.postgres_connector import SupabaseQueryExecutor
from includes.utils import serialize_results, serialize_value
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
        # Table name
        table_name:str = "salle"
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
        query = query_manager.get_query_params(file_path="select_all_table.sql", parameters={ "table_name" : table_name })
       
        # Execute query (connection is auto-closed by executor)
        results = executor.execute(query)
        
        # Create DataFrame and load to MotherDuck
        df = pd.DataFrame(results)
        con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        # Serialize results for XCom
        serialized_results = serialize_results(results)
        
        # Return summary instead of full results to avoid XCom size issues
        return {
            'status': 'success',
            'rows_processed': len(serialized_results),
            'timestamp': datetime.now().isoformat(),
            'sample_data': serialized_results[:5] if serialized_results else []  # Only return first 5 rows
        }
        
    except Exception as e:
        # Log the error and re-raise
        print(f"Error querying Supabase: {str(e)}")
        raise
    finally:
        # Ensure MotherDuck connection is closed
        if 'con' in locals():
            try:
                con.close()
            except Exception as e:
                print(f"Error closing DuckDB connection: {e}")

# Create the DAG
dag = DAG(
    'export_salle_dag',
    default_args=default_args,
    description='Simple DAG to query Supabase database',
    schedule='0 3 * * *',  # Updated from schedule_interval
    catchup=False,
    tags=['supabase', 'database'],
)

# Define the task
query_task = PythonOperator(
    task_id='export_salle_dag',
    python_callable=query_supabase,
    dag=dag,
)

if __name__ == "__main__":
    try:
        results = query_supabase()
        print(f"Results: {results}")
    except Exception as e:
        print(f"Failed to query database: {str(e)}")
