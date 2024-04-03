import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.expanduser("~/BDL/A02/"))

from utils.pipeline_2 import (
    make_heatmaps,
    process_df_hourly_data_array,
    process_df_monthly_avg,
    update_inputs,
)

working_dir = os.path.expanduser("~/BDL/A02/")
os.makedirs(working_dir + "tmp", exist_ok=True)

# create dag
with DAG(
    dag_id="analytics_sahilll",
    schedule="@daily",
    start_date=datetime(year=2022, month=2, day=19),
    end_date=datetime(year=2025, month=3, day=18),
    catchup=False,
    tags=["test"],
) as dag:
    
    # Get the inputs from the user
    task_update_inputs = PythonOperator(
        task_id="update_inputs", python_callable=update_inputs, dag=dag
    )

    # Create variable to get the zip file path entered from user
    filepath = Variable.get("archive_file_path", default_var="/ARCCC/data_files.zip")
    logging.info(f"filepath later is {filepath}")

    # Wait for the archive to be available
    task_file_sensor = FileSensor(
        task_id="file_sensor",
        filepath=working_dir + filepath,
        poke_interval=10,
        timeout=5,
        dag=dag,
    )

    # Check if the file is a valid archive followed by unzip
    task_validate_and_unzip = BashOperator(
        task_id="validate_and_unzip",
        bash_command=f"""
        if [ -f {working_dir + filepath} ]; then
            unzip -o {working_dir + filepath} -d {working_dir}/tmp/
            echo "success"
        else 
            echo "error"
            exit 1
        fi 
        """,
        dag=dag,
    )

    # Extract CSV into data frame also the Lat/Long values in tuple
    task_process_df_hourly = PythonOperator(
        task_id="process_df_hourly_data_array",
        python_callable=process_df_hourly_data_array,
    )

    # compute the monthly averages of the required fields in tuple
    task_process_df_monthly_avg = PythonOperator(
        task_id="process_df_monthly_avg", python_callable=process_df_monthly_avg
    )

    # generate heatmaps for the required fields
    task_make_heatmaps = PythonOperator(
        task_id="make_heatmaps", python_callable=make_heatmaps
    )

    # delete tmp directory
    task_delete_tmp_dir = BashOperator(
        task_id="delete_tmp_directory",
        bash_command=f"""rm -r {working_dir}/tmp""",
        dag=dag,
    )

    # define dependencies
    (
        task_update_inputs
        >> task_file_sensor
        >> task_validate_and_unzip
        >> task_process_df_hourly
        >> task_process_df_monthly_avg
        >> task_make_heatmaps
        >> task_delete_tmp_dir
    ) # type:ignore
