from pathlib import Path
from datetime import datetime, timedelta
import duckdb
from dotenv import dotenv_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from includes.query_manager import QueryManager

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

# List of SQL files to execute in order
SQL_FILES = [
    "facts/fact_evenement_sante_init.sql",
    "facts/fact_evenement_sante_drop_data.sql",
    "dimensions/dim_diagnostic.sql",
    "dimensions/dim_etablissement.sql",
    "dimensions/dim_localisation.sql",
    "dimensions/dim_mutuelle.sql",
    "dimensions/dim_patient.sql",
    "dimensions/dim_professionnel.sql",
    "dimensions/dim_temps.sql",
]

# List of SQL files to execute after 30 minutes
SQL_FILES_AFTER_WAIT = [
    "facts/fact_evenement_sante.sql",
]

def execute_sql_files(sql_files_list, task_name="query_execution"):
    """Execute multiple SQL scripts against MotherDuck"""
    _env = dotenv_values(dotenv_path=Path('.env'))
    con = None
    results_summary = []
   
    try:      
        ### --- MotherDuck --- ###
        motherduck_database_name = "warehouse"
        motherduck_token = _env.get('MOTHERDUCK_TOKEN', '')
        con = duckdb.connect(f'md:{motherduck_database_name}?motherduck_token={motherduck_token}')
       
        ### --- S3 credentials --- ###
        s3_key_id = _env.get('S3_KEY_ID', '')
        s3_secret = _env.get('S3_SECRET', '')
        s3_endpoint = _env.get('S3_ENDPOINT', '')
        s3_scope = _env.get('S3_SCOPE', '')
        
        # Parameters to inject into all queries
        parameters = {
            "database_name" : f"{motherduck_database_name}",
            "s3_key_id": f"'{s3_key_id}'",
            "s3_secret": f"'{s3_secret}'",
            "s3_endpoint": f"'{s3_endpoint}'",
            "s3_scope": f"'{s3_scope}'",
        }
        
        # Initialize query manager
        query_manager = QueryManager(queries_directory="./dags/queries/olap-motherduck")
        
        # Execute each SQL file
        for sql_file in sql_files_list:
            try:
                # Get query with parameters
                query = query_manager.get_query_params(
                    file_path=sql_file,
                    parameters=parameters
                )
                print(query)
                # Execute the query
                result = con.execute(query).fetchall()
                
                # Track results
                file_result = {
                    'file': sql_file,
                    'status': 'success',
                    'rows_affected': len(result) if result else 0,
                    'timestamp': datetime.now().isoformat()
                }
                results_summary.append(file_result)
                print(f"✓ {sql_file} completed: {len(result) if result else 0} rows")
                
            except Exception as e:
                error_result = {
                    'file': sql_file,
                    'status': 'failed',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                results_summary.append(error_result)
                print(f"✗ {sql_file} failed: {str(e)}")
        
        # Return comprehensive summary
        return {
            'status': 'completed',
            'task_name': task_name,
            'total_files': len(sql_files_list),
            'successful': sum(1 for r in results_summary if r['status'] == 'success'),
            'failed': sum(1 for r in results_summary if r['status'] == 'failed'),
            'details': results_summary,
            'execution_time': datetime.now().isoformat()
        }
       
    except Exception as e:
        print(f"Error in {task_name}: {str(e)}")
        raise
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                print(f"Error closing DuckDB connection: {e}")

def update_olap():
    """Execute initial SQL scripts"""
    return execute_sql_files(SQL_FILES, "initial_olap_update")

def update_olap_delayed():
    """Execute delayed SQL scripts"""
    return execute_sql_files(SQL_FILES_AFTER_WAIT, "delayed_olap_update")

# Create the DAG
dag = DAG(
    'trigger_olap_update_dag',
    default_args=default_args,
    description='Execute multiple SQL scripts against MotherDuck with delayed execution',
    schedule='0 6 * * *',
    catchup=False,
    tags=['motherduck', 's3', 'etl'],
)

# Define the initial task
initial_query_task = PythonOperator(
    task_id='initial_olap_update',
    python_callable=update_olap,
    dag=dag,
)

# Define the 30-minute wait sensor
wait_30_minutes = TimeDeltaSensor(
    task_id='wait_30_minutes',
    delta=timedelta(minutes=30),
    dag=dag,
)

# Define the delayed task
delayed_query_task = PythonOperator(
    task_id='delayed_olap_update',
    python_callable=update_olap_delayed,
    dag=dag,
)

# Set task dependencies
initial_query_task >> wait_30_minutes >> delayed_query_task

if __name__ == "__main__":
    try:
        print("Running initial SQL files...")
        results = update_olap()
        for detail in results['details']:
            status_symbol = '✓' if detail['status'] == 'success' else '✗'
            print(f"{status_symbol} {detail['file']}")
        
        print("\nWaiting 30 minutes...")
        import time
        time.sleep(30 * 60)  # 30 minutes in seconds
        
        print("\nRunning delayed SQL files...")
        delayed_results = update_olap_delayed()
        for detail in delayed_results['details']:
            status_symbol = '✓' if detail['status'] == 'success' else '✗'
            print(f"{status_symbol} {detail['file']}")
            
    except Exception as e:
        print(f"Failed to execute SQL scripts: {str(e)}")
