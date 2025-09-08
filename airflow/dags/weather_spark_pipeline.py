from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


PROJECT_DIR = "/Users/baburammarandi/Documents/weather_batch_project"
VENV_ACTIVATE = f"{PROJECT_DIR}/my_venv/bin/activate"
PYTHON = f"{PROJECT_DIR}/my_venv/bin/python3"
SPARK_SUBMIT = f"{PROJECT_DIR}/my_venv/bin/spark-submit"
GOOGLE_CREDENTIALS = f"{PROJECT_DIR}/weather-batch-project-21efd4465537.json"
GCS_JAR = "/Users/baburammarandi/jars/gcs-connector-hadoop3-2.2.5-shaded.jar"
BQ_JAR = "/Users/baburammarandi/jars/spark-bigquery-with-dependencies_2.12-0.31.0.jar"


JAVA_11_HOME = '/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'weather_spark_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id='extract_weather_data',
        bash_command=f"""
        source {VENV_ACTIVATE} && cd {PROJECT_DIR} && \
        {PYTHON} extraction/weather_extract.py
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_CREDENTIALS,
            'PATH': (
                f'{JAVA_11_HOME}/bin:'  # Java 11 bin
                f'{PROJECT_DIR}/my_venv/bin:'  # virtualenv bin
                '/usr/local/bin:/usr/bin:/bin'  # system paths
            ),
            'JAVA_HOME': JAVA_11_HOME,
        }
    )

    upload_task = BashOperator(
        task_id='upload_extracted_data',
        bash_command=f"""
        source {VENV_ACTIVATE} && cd {PROJECT_DIR} && \
        {PYTHON} upload/upload_to_gcs.py
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_CREDENTIALS,
            'PATH': (
                f'{JAVA_11_HOME}/bin:'
                f'{PROJECT_DIR}/my_venv/bin:'
                '/usr/local/bin:/usr/bin:/bin'
            ),
            'JAVA_HOME': JAVA_11_HOME,
        }
    )

    run_spark_job = BashOperator(
        task_id='run_spark_processing',
        bash_command=f"""
        source {VENV_ACTIVATE} && cd {PROJECT_DIR} && \
        {SPARK_SUBMIT} \
          --jars {GCS_JAR},{BQ_JAR} \
          --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
          --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
          --conf spark.hadoop.fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE \
          --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile={GOOGLE_CREDENTIALS} \
          --conf spark.driver.extraJavaOptions=-Dorg.apache.hadoop.util.NativeCodeLoader.disable=true \
          --conf spark.executor.extraJavaOptions=-Dorg.apache.hadoop.util.NativeCodeLoader.disable=true \
          dataflow/weather_spark.py
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_CREDENTIALS,
            'JAVA_HOME': JAVA_11_HOME,
            'PATH': (
                f'{JAVA_11_HOME}/bin:'
                f'{PROJECT_DIR}/my_venv/bin:'
                '/usr/local/bin:/usr/bin:/bin'
            ),
        },
        dag=dag,
    )

    extract_task >> upload_task >> run_spark_job
