from pathlib import Path
from datetime import datetime, timedelta
import duckdb
from dotenv import dotenv_values
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator
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
    "s3_files_script.sql",
    "create_view_activite_professionnel_sante.sql",
    "create_view_deces.sql",
    "create_view_etablissement_sante.sql",
    "create_view_hospitalisation.sql",
    "create_view_lexique.sql",
    "create_view_professionnel_sante.sql",
    "create_view_recueil.sql",
]

def s3_trigger():
    """Execute multiple SQL scripts against MotherDuck"""
    _env = dotenv_values(dotenv_path=Path('.env'))
    con = None
    results_summary = []
   
    try:      
        ### --- MotherDuck --- ###
        motherduck_database_name = "lakehouse"
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
        query_manager = QueryManager(queries_directory="./dags/queries/s3-motherduck")
        
        # Execute each SQL file
        for sql_file in SQL_FILES:
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
        
        # Return comprehensive summary
        return {
            'status': 'completed',
            'total_files': len(SQL_FILES),
            'successful': sum(1 for r in results_summary if r['status'] == 'success'),
            'failed': sum(1 for r in results_summary if r['status'] == 'failed'),
            'details': results_summary,
            'execution_time': datetime.now().isoformat()
        }
       
    except Exception as e:
        print(f"Error in s3_trigger: {str(e)}")
        raise
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                print(f"Error closing DuckDB connection: {e}")

# Create the DAG
dag = DAG(
    'trigger_s3_exports_dag',
    default_args=default_args,
    description='Execute multiple SQL scripts against MotherDuck',
    schedule='0 5 * * *',
    catchup=False,
    tags=['motherduck', 's3', 'etl'],
)

# Define the task
query_task = PythonOperator(
    task_id='execute_s3_sql_scripts',
    python_callable=s3_trigger,
    dag=dag,
)

# Wait for 30 minutes
wait_task = TimeDeltaSensor(
    task_id='wait_30_minutes',
    delta=timedelta(minutes=30),
    dag=dag,
)

# Trigger the next DAG
trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_olap_update_dag',
    trigger_dag_id='trigger_olap_update_dag',
    wait_for_completion=False,
    dag=dag,
)

# Set task dependencies
query_task >> wait_task >> trigger_next_dag

if __name__ == "__main__":
    try:
        results = s3_trigger()
        for detail in results['details']:
            status_symbol = '✓' if detail['status'] == 'success' else '✗'
    except Exception as e:
        print(f"Failed to execute SQL scripts: {str(e)}")
