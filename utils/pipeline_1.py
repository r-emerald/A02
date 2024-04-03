import glob
import logging
import os

import apache_beam as beam  
import geopandas as gpd  
import matplotlib.pyplot as plt  
import numpy as np  
import pandas as pd  
from shapely.geometry import Point  

from airflow.models import Variable as AirflowVariable

# Setup the working directory
working_dir_path = os.path.expanduser("~/BDL/A02/")
if not os.path.exists(working_dir_path + "tmp"):
    print(f"Existing tmp dir not found, creating: {working_dir_path + 'tmp'}")
    os.makedirs(working_dir_path + "tmp")


def update_inputs(**kwargs):
    """
    Update the input variables using the provided keyword arguments.
    """
    try:
        AirflowVariable.set("archive_file_path", kwargs["dag_run"].conf["archive_file_path"])
        AirflowVariable.set("required_cols", kwargs["dag_run"].conf["required_cols"])
        logging.info(
            "input fields archive_file_path and required_cols read successfully"
        )
    except KeyError:
        logging.error("""archive_file_path or required_cols is not entered correctly, ensure you pass
                      config args as --conf '{"archive_file_path": "<archive_file_path>", "required_cols": ["col1", "col2"]}'
                      """
                      )
        exit()
    return


def process_df_hourly_data_array(**kwargs):
    """
    A function to process hourly data array from CSV files and write the aggregated data to a text file.
    """

    # Proceed only if the previous task succeeded
    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")
    if status == "error":
        logging.error("failed to unzip the data")
        exit()


    logging.info("successfully unzipped the data")

    # Use glob to find all CSV files in the directory
    csv_files = glob.glob(f"{working_dir_path}/tmp/*.csv")

    # Display the list of CSV file paths
    logging.info(f"csv_files read from glob are {csv_files} ")

    required_columns = AirflowVariable.get("required_cols", default_var=["ELEVATION"])
    logging.info(f"required_columns input by user is {required_columns}")

    # Convert the string input to a list
    required_columns = eval(required_columns)

    def read_csv(file_name):
        """
        Read CSV file(s) and return a pandas dataframe with selected cols.
        If required_cols are not in the dataframe, raise an error.
        """
        # if columns_to_read not in the dataframe
        columns_to_read = required_columns + ["LATITUDE", "LONGITUDE"]

        columns_in_df = pd.read_csv(csv_files[0]).columns.tolist()
        columns_to_read_not_in_df = [i for i in columns_to_read if i not in columns_in_df]

        if columns_to_read_not_in_df:
            logging.error(
                f"Specified columns not found in CSV file: {columns_to_read_not_in_df}"
            )
            exit()
        else:
            logging.info("All specified columns found in CSV file")

        return pd.read_csv(file_name, usecols=columns_to_read)

    def make_tuples(df):
        """
        Create tuples containing latitude, longitude, and aggregated hourly data from the input dataframe.
        """

        # Initialize an empty list to store the aggregated hourly data
        aggregated_hourly_data = []

        # Iterate over groups
        for (lat, lon), group in df.groupby(["LATITUDE", "LONGITUDE"]):
            # Extract required hourly data for each group
            hourly_data = group[required_columns].values.tolist()
            aggregated_hourly_data.extend(hourly_data)

        # Create a tuple containing latitude, longitude, and aggregated hourly data
        result = (
            df["LATITUDE"].iloc[0],
            df["LONGITUDE"].iloc[0],
            aggregated_hourly_data,
        )
        return result

    # Write output to tmp dir
    output_path = working_dir_path + "/tmp/" + "values_arrays.txt"
    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_csv) | beam.Map(make_tuples)
        )
        csv_data | beam.io.WriteToText(
            output_path,
            num_shards=1,
        )

    return


def process_df_monthly_avg(**kwargs):
    """
    Function to process monthly average data from CSV files and write the results to a text file.
    """

    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")

    if status == "error":
        logging.error("failed to unzip the data")
        exit()

    logging.info("successfully unzipped the data")

    # Display the list of CSV file paths
    csv_files = glob.glob(f"{working_dir_path}/tmp/*.csv")
    logging.info(f"csv_files read from glob are {csv_files} ")
    required_columns = AirflowVariable.get("required_cols", default_var=["HourlyWindSpeed"])
    logging.info(f"required_columns input by user is {required_columns}")

    # Convert the string input to a list
    required_columns = eval(required_columns)

    def read_csv(file_name):
        """
        Read CSV file(s) and return a pandas dataframe with selected cols.
        If required_cols are not in the dataframe, raise an error.
        """
        columns_to_read = required_columns + ["DATE", "LATITUDE", "LONGITUDE"]
        columns_in_df = pd.read_csv(csv_files[0]).columns.tolist()
        columns_to_read_not_in_df = [i for i in columns_to_read if i not in columns_in_df]
        if columns_to_read_not_in_df:
            logging.error(
                f"Specified columns not found in CSV file: {columns_to_read_not_in_df}"
            )
            exit()
        else:
            logging.info("All specified columns found in CSV file")
        return pd.read_csv(file_name, usecols=columns_to_read)

    def monthly_avg(df):
        """
        Calculate the monthly average values for the given dataframe.
        """

        lat = df["LATITUDE"].iloc[0]
        lon = df["LONGITUDE"].iloc[0]

        data = df.values.tolist()
        monthly_avg = []
        required_columns_idx = [df.columns.get_loc(c) for c in required_columns]

        # Calculate monthly average for every col in loop
        for col in required_columns_idx:
            col_monthly_avg = []
            for month in range(1, 13):
                # Extract data for each month by splitting the data column by "-"
                month_data = [x[col] for x in data if int(x[0].split("-")[1]) == month]

                # Remove any non-numeric values and convert to float
                month_data = np.array(
                    [
                        float(x) if str(x).replace(".", "", 1).isdigit() else np.nan
                        for x in month_data
                    ],
                    dtype=np.float32,
                )

                # Take nanmean to ignore null values
                col_avg = np.nanmean(month_data)
                col_monthly_avg.append(col_avg)

            monthly_avg.append(col_monthly_avg)
            logging.info(f"Calculating monthly_avg as {monthly_avg}")

        # Convert it to required format 
        # i.e. each little list has values of all given cols for that month
        monthly_avg = [list(x) for x in zip(*monthly_avg)]

        logging.info(f"RETURNING {(lat, lon, monthly_avg)}")
        return (lat, lon, monthly_avg)

    output_path = working_dir_path + "/tmp" + "/values_averages.txt"

    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_csv) | beam.Map(monthly_avg)
        )

        csv_data | beam.io.WriteToText(
            output_path,
            num_shards=1,
        )
    return


def make_heatmaps(**kwargs):
    """
    Make heatmaps using the given data. Reads the data from a file, processes it, 
    generates the heatmaps for specified fields, and saves the output as image files.
    """

    # Open the output from previous file
    reading_path = working_dir_path + "/tmp" + "/values_averages.txt" + "-00000-of-00001"
    with open(reading_path, "r") as file:
        content = file.readlines()
    logging.info(f"content is \n{content}")

    # Convert nan to python-understandable None
    content = [i.replace("nan", "None") for i in content]

    # Convert the string input from the file to tuples using eval
    data = [eval(line.strip()) for line in content]
    logging.info("read the tuple from the previous python operator")

    def process_data(data):
        """
        Process the given data to extract latitude, longitude, and monthly averages.
        """
        latitudes = [entry[0] for entry in data]
        longitudes = [entry[1] for entry in data]
        monthly_averages = [entry[2] for entry in data]

        logging.info("Extracted coordinates and data values from the tuple")
        return latitudes, longitudes, monthly_averages

    def generate_heatmap(latitudes, longitudes, monthly_averages, required_columns):
        """
        Generate heatmap from given latitude, longitude, and monthly average data.
        """

        # Create a GeoDataFrame with latitude, longitude, and field averages
        geometry = [Point(lon, lat) for lon, lat in zip(longitudes, latitudes)]

        df = pd.DataFrame(
            {
                "Latitude": latitudes,
                "Longitude": longitudes,
                "MonthlyAverages": monthly_averages,
            }
        )

        # Plot the values for the first month for now
        for i, field_name in enumerate(required_columns):
            df[field_name] = [monthly_avg[0][i] for monthly_avg in monthly_averages]

        # Create a GeoDataFrame
        crs = {"init": "epsg:4326"}
        gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        logging.info(f"geopandas df is \n{gdf}")

        # Plot heatmaps for each field
        for i, field_name in enumerate(required_columns):
            
            # Get world map file 
            world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            fig, ax = plt.subplots(figsize=(12, 8))
            world.plot(ax=ax, color="white", edgecolor="gray")

            # Plot the values only if the field is not null
            if gdf[field_name].notnull().any():
                gdf.plot(
                    column=field_name, cmap="coolwarm", linewidth=6, ax=ax, legend=True
                )
                ax.set_title(
                    f"Heatmap of {field_name}\nfor given stations" ,
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

            # Save plot to output_heatmaps directory
            os.makedirs(working_dir_path + "/output_heatmaps", exist_ok=True)
            plt.savefig(
                working_dir_path + "/output_heatmaps" + f"/heatmap_geo_{field_name}.png"
            )

    def run_pipeline(data):
        """
        This function runs a pipeline to process data, retrieve required columns, and generate heatmaps.
        """

        latitudes, longitudes, monthly_averages = process_data(data)
        required_columns = AirflowVariable.get("required_cols", default_var=["HourlyWindSpeed"])
        required_columns = eval(required_columns)
        logging.info("Starting to generate the heatmaps")
        generate_heatmap(latitudes, longitudes, monthly_averages, required_columns)

    # Run pipeline
    with beam.Pipeline(runner="DirectRunner") as pipeline:
        p = (
            pipeline
            | "Create data" >> beam.Create([data])
            | "Run pipeline" >> beam.Map(run_pipeline)
        )

    return
