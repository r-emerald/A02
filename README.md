# A02
Creating Pipelines in Apache Beam!

Before proceeding, it is recommended to create a virtual environment using venv.
To install the required packages, use the following command: ```bash
pip install -r requirements.txt

Development was done on WSL2 on Windows 10.
Run all terminal commands given below using the Bash terminal, preferably being in the ~/BDL/A02 directory.

## Installing Airflow
1. Airflow was installed using the following command:pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

## Setting up Airflow
export AIRFLOW_HOME=~/airflow
1. Ensure that the dags_folder is set correctly in the airflow config file /root/airflow/airflow.cfg by setting dags_folder = /root/BDL/A02/dags.
2. Start the webserver in one terminal: airflow webserver -p 8080
3. Run the scheduler in another terminal: airflow scheduler

## Running the code
Datafetch pipeline: airflow dags trigger 'datafetch_custom' --conf '{"year":2019, "num_samples":5, "archive_dir": "ARCHIVE"}'
Analytics pipeline: airflow dags trigger 'analytics_custom'  --conf '{"archive_file_path": "ARCHIVE/data_files.zip", "required_cols": ["HourlyWindSpeed", "HourlyDryBulbTemperature", "HourlyRelativeHumidity"]}'

Notes:
1. Plots will be stored in the output_heatmaps directory.
2. Validation checks are done within folders, mostly as WARNING and ERROR log messages.
3. If the pipeline does not trigger, run airflow dags list-import-errors and then trigger the pipelines again.
4. External installation of the unzip command may be required using sudo apt-get install unzip.
