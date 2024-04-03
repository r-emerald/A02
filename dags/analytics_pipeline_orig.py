import glob
import logging
import os
import sys
from datetime import datetime

import apache_beam as beam
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.expanduser("~/BDL/A02/"))

working_dir = os.path.expanduser("~/BDL/A02/")
os.makedirs(working_dir + "tmp", exist_ok=True)

def custom_update_inputs(**kwargs):
    try:
        Variable.set("data_archive_path", kwargs["dag_run"].conf["data_archive_path"])
        Variable.set("selected_columns", kwargs["dag_run"].conf["selected_columns"])
        logging.info("Input fields 'data_archive_path' and 'selected_columns' read successfully.")
    except KeyError:
        logging.error("""Provided data_archive_path or selected_columns is empty. Ensure you pass
                                config args as --conf '{"data_archive_path": "<data_archive_path>", "selected_columns": ["col1", "col2"]}'
                                """)
        exit()
    return


def custom_process_df_hourly_data_array(**kwargs):
    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")
    if status == "error":
        logging.error("Failed to extract the data from the archive.")
        exit()

    logging.info("Data extraction successful.")
    csv_files = glob.glob(f"{working_dir}/tmp/*.csv")

    logging.info(f"CSV files identified: {csv_files}")

    selected_columns = Variable.get("selected_columns", default_var=["ELEVATION"])
    logging.info(f"Selected columns provided by user: {selected_columns}")

    selected_columns = eval(selected_columns)

    def read_csv(file_name):
        columns_in_file = pd.read_csv(csv_files[0]).columns.tolist()
        missing_columns = [col for col in selected_columns if col not in columns_in_file]

        if missing_columns:
            logging.error(
                f"Specified columns not found in CSV file: {missing_columns}"
            )
            exit()
        else:
            logging.info("All specified columns found in CSV file.")
        return pd.read_csv(file_name, usecols=selected_columns)

    def prepare_tuples(df):
        aggregated_data = []

        for (lat, lon), group in df.groupby(["LATITUDE", "LONGITUDE"]):
            hourly_data = group[selected_columns].values.tolist()
            aggregated_data.extend(hourly_data)

        result = (df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0], aggregated_data)
        return result

    output_path = working_dir + "/tmp/" + "hourly_values_arrays.txt"
    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_csv) | beam.Map(prepare_tuples)
        )
        csv_data | beam.io.WriteToText(
            output_path,
            num_shards=1,
        )

    return


def custom_process_df_monthly_avg(**kwargs):
    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")

    if status == "error":
        logging.error("Failed to extract the data from the archive.")
        exit()

    logging.info("Data extraction successful.")

    csv_files = glob.glob(f"{working_dir}/tmp/*.csv")

    logging.info(f"CSV files identified: {csv_files}")

    selected_columns = Variable.get("selected_columns", default_var=["HourlyWindSpeed"])
    logging.info(f"Selected columns provided by user: {selected_columns}")

    selected_columns = eval(selected_columns)

    def read_csv(file_name):
        columns_in_file = pd.read_csv(csv_files[0]).columns.tolist()
        missing_columns = [col for col in selected_columns if col not in columns_in_file]

        if missing_columns:
            logging.error(
                f"Specified columns not found in CSV file: {missing_columns}"
            )
            exit()
        else:
            logging.info("All specified columns found in CSV file.")
        return pd.read_csv(file_name, usecols=selected_columns)

    def calculate_monthly_avg(df):
        lat = df["LATITUDE"].iloc[0]
        lon = df["LONGITUDE"].iloc[0]

        data = df.values.tolist()
        monthly_avg = []

        selected_columns_idx = [df.columns.get_loc(c) for c in selected_columns]

        for col in selected_columns_idx:
            col_monthly_avg = []
            for month in range(1, 13):
                month_data = [x[col] for x in data if int(x[0].split("-")[1]) == month]
                month_data = np.array(
                    [
                        float(x) if str(x).replace(".", "", 1).isdigit() else np.nan
                        for x in month_data
                    ],
                    dtype=np.float32,
                )
                col_avg = np.nanmean(month_data)
                col_monthly_avg.append(col_avg)
            monthly_avg.append(col_monthly_avg)

        monthly_avg = [list(x) for x in zip(*monthly_avg)]

        return (lat, lon, monthly_avg)

    output_path = working_dir + "/tmp" + "/monthly_averages.txt"

    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_csv) | beam.Map(calculate_monthly_avg)
        )

        csv_data | beam.io.WriteToText(
            output_path,
            num_shards=1,
        )
    return


def custom_make_heatmaps(**kwargs):
    reading_path = working_dir + "/tmp" + "/monthly_averages.txt" + "-00000-of-00001"
    with open(reading_path, "r") as file:
        content = file.readlines()

    logging.info(f"Content read from file: \n{content}")

    content = [i.replace("nan", "None") for i in content]

    logging.info("Data processing started.")
    data = [eval(line.strip()) for line in content]

    def process_data(data):
        latitudes = [entry[0] for entry in data]
        longitudes = [entry[1] for entry in data]
        monthly_averages = [entry[2] for entry in data]

        logging.info("Data extracted from the input tuples.")
        return latitudes, longitudes, monthly_averages

    def generate_heatmap(latitudes, longitudes, monthly_averages, selected_columns):
        geometry = [Point(lon, lat) for lon, lat in zip(longitudes, latitudes)]

        df = pd.DataFrame(
            {
                "Latitude": latitudes,
                "Longitude": longitudes,
                "MonthlyAverages": monthly_averages,
            }
        )
        crs = {"init": "epsg:4326"}

        for i, field_name in enumerate(selected_columns):
            df[field_name] = [monthly_avg[0][i] for monthly_avg in monthly_averages]

        gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

        for i, field_name in enumerate(selected_columns):
            world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            fig, ax = plt.subplots(figsize=(12, 8))
            world.plot(ax=ax, color="white", edgecolor="gray")

            if gdf[field_name].notnull().any():
                gdf.plot(
                    column=field_name, cmap="coolwarm", linewidth=6, ax=ax, legend=True
                )
                ax.set_title(
                    f"Heatmap of {field_name}\nfor given {df.shape[0]} stations",
                    fontdict={"fontsize": "18", "fontweight": "3"},
                )
            else:
                ax.set_title(
                    f"Heatmap of {field_name} for given {df.shape[0]} stations.\nNo data available for this field.",
                    fontdict={"fontsize": "15", "fontweight": "3"},
                )

            ax.autoscale()
            ax.axis("off")
            fig.tight_layout()

            os.makedirs(working_dir + "/output_heatmaps", exist_ok=True)
            plt.savefig(
                working_dir + "/output_heatmaps" + f"/heatmap_geo_{field_name}.png"
            )

    def run_pipeline(data):
        latitudes, longitudes, monthly_averages = process_data(data)
        selected_columns = Variable.get("selected_columns", default_var=["HourlyWindSpeed"])
        selected_columns = eval(selected_columns)
        generate_heatmap(latitudes, longitudes, monthly_averages, selected_columns)

    with beam.Pipeline(runner="DirectRunner") as pipeline:
        p = (
            pipeline
            | "Create data" >> beam.Create([data])
            | "Run pipeline" >> beam.Map(run_pipeline)
        )

    return


with DAG(
    dag_id="analytics_modified",
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 19),
    end_date=datetime(2025, 3, 18),
    catchup=False,
    tags=["test"],
) as dag:
    
    task_update_inputs = PythonOperator(
        task_id="update_inputs", python_callable=custom_update_inputs, dag=dag
    )

    data_filepath = Variable.get("data_archive_path", default_var="/ARCHIVE/data_files.zip")
    logging.info(f"Data filepath: {data_filepath}")

    if os.path.exists(data_filepath):
        logging.warning(f"The file at {data_filepath} exists.")
    else:
        error_msg = f"The file at {data_filepath} does not exist."
        logging.error(error_msg)

    task_wait_for_archive = FileSensor(
        task_id="wait_for_archive",
        filepath=working_dir + data_filepath,
        poke_interval=10,
        timeout=5,
        dag=dag,
    )

    task_validate_and_unzip = BashOperator(
        task_id="validate_and_unzip",
        bash_command=f"""
        if [ -f {working_dir + data_filepath} ]; then
            unzip -o {working_dir + data_filepath} -d {working_dir}/tmp/
            echo "success"
        else 
            echo "error"
        fi 
        """,
        dag=dag,
    )

    task_process_df_hourly = PythonOperator(
        task_id="process_df_hourly_data_array",
        python_callable=custom_process_df_hourly_data_array,
    )

    task_process_df_monthly_avg = PythonOperator(
        task_id="process_df_monthly_avg", python_callable=custom_process_df_monthly_avg
    )

    task_make_heatmaps = PythonOperator(
        task_id="make_heatmaps", python_callable=custom_make_heatmaps
    )

    task_delete_tmp_dir = BashOperator(
        task_id="delete_tmp_directory",
        bash_command=f"""rm -r {working_dir}/tmp""",
        dag=dag,
    )

    (
        task_update_inputs
        >> task_wait_for_archive
        >> task_validate_and_unzip
        >> task_process_df_hourly
        >> task_process_df_monthly_avg
        >> task_make_heatmaps
        >> task_delete_tmp_dir
    )
