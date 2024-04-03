import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# sys.path.append('~/BDL/A02/')
sys.path.append(os.path.expanduser("~/BDL/A02/"))

from custom.pipeline_functions import (
    fetch_files,
    move_archive,
    select_random_files,
    create_archive,
)

working_dir = os.path.expanduser("~/BDL/A02/")


# create dag
with DAG(
    dag_id="datafetch_custom",
    schedule_interval="@daily",
    start_date=datetime(year=2022, month=2, day=19),
    end_date=datetime(year=2025, month=3, day=18),
    catchup=False,
    tags=["test"],
) as dag:

    # Task to fetch page contents using wget
    task_fetch_page = BashOperator(
        task_id="fetch_page",
        bash_command="""wget -O ~/BDL/A02/tmp/datasets_custom.html 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{{dag_run.conf["year"] if dag_run else ""}}'
        """,
    )

    # Task to select files from fetched HTML
    task_select_files = PythonOperator(
        task_id="select_random_files",
        python_callable=select_random_files,
        provide_context=True,
    )

    # Task to download the selected files
    task_fetch_files = PythonOperator(
        task_id="fetch_files",
        python_callable=fetch_files,
        provide_context=True,
    )

    # Task to create zip archive
    task_create_archive = PythonOperator(
        task_id="create_archive",
        python_callable=create_archive,
        provide_context=True,
    )

    # Task to move archive to a location
    task_move_archive = PythonOperator(
        task_id="move_archive",
        python_callable=move_archive,
        provide_context=True,
    )

    # Task to delete the tmp directory
    task_delete_tmp_dir = BashOperator(
        task_id="delete_tmp_directory",
        bash_command=f"""rm -r {working_dir}/tmp""",
        dag=dag,
    )

    # Define dependencies
    (
        task_fetch_page
        >> task_select_files
        >> task_fetch_files
        >> task_create_archive
        >> task_move_archive
        >> task_delete_tmp_dir
    )
