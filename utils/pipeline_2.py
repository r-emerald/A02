import logging
import os
import random
import shutil

import requests
from bs4 import BeautifulSoup

# Setup the working directory
working_directory = os.path.expanduser("~/BDL/A02/")

if not os.path.exists(working_directory + "tmp"):
    logging.info(f"Existing temporary directory not found, creating: {working_directory + 'tmp'}")
    os.makedirs(working_directory + "tmp")


def select_random_files_from_fetched_html(**kwargs):
    """
    Selects random files from fetched HTML content and returns their URLs.
    """
    # Fetch user input
    selected_year = str(kwargs["dag_run"].conf["selected_year"])

    if 1901 <= int(selected_year) <= 2024:
        logging.warning("Year is validated within the range 1901 to 2024")
    else:
        logging.error("Year is not within the range 1901 to 2024")
        exit()

    num_samples = kwargs["dag_run"].conf["num_samples"]

    if num_samples > 1:
        logging.warning("Number of samples is validated to be greater than 1")
    else:
        logging.error("Number of samples is not greater than 0")
        exit()

    logging.info("Fetched user input of year and number of samples")

    main_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"

    # Get the full path to the HTML file
    html_file_path = working_directory + "tmp/datasets_new.html"

    with open(html_file_path, "r") as f:
        html_content = f.read()

    # Parse HTML content to extract CSV file URLs
    soup = BeautifulSoup(html_content, "html.parser")
    csv_links = [
        link["href"] for link in soup.find_all("a") if link["href"].endswith(".csv")
    ]
    logging.info("Parsed HTML content to extract CSV file URLs")

    # Sample a given number of CSV file URLs
    sampled_csv_links = random.sample(csv_links, min(num_samples, len(csv_links)))

    formatted_urls = [main_url + selected_year + "/" + i for i in sampled_csv_links]
    logging.info(f"Formatted URLs are {formatted_urls}")

    return formatted_urls


def fetch_individual_files(ti):
    """
    Fetch individual files based on the input ti. It retrieves output from the previous task,
    downloads sampled CSV files, and saves them in the specified directory.
    """

    # Get output from previous task
    links = ti.xcom_pull(task_ids=["select_random_files_from_fetched_html"])
    links = links[0]  # The xcom pull returns a list of lists
    logging.info(f"Fetched links are {links}")

    # Download sampled CSV files
    save_path = working_directory + "tmp/csv_files/"
    if not os.path.exists(save_path):
        logging.info(f"Existing data not found, creating: {save_path}")
        os.makedirs(save_path)
    else:
        logging.info(f"Existing data found at {save_path}, clearing...")
        shutil.rmtree(save_path)
        os.makedirs(save_path)

    for url in links:
        filename = url.split("/")[-1]
        with open(f"{save_path}/{filename}", "wb") as f:
            response = requests.get(url)
            f.write(response.content)

    logging.info("Downloaded the files")
    return


def zip_files_into_archive():
    """
    Create a zip archive of the files in the 'tmp/csv_files' directory
    and save it to the 'tmp/archive_files' directory.
    """

    dir_name = "zip_archive"
    dir_path = working_directory + dir_name
    if not os.path.exists(dir_path):
        print("Creating zip directory")
        os.makedirs(dir_path)

    source_path = working_directory + "tmp/csv_files/"
    destination_path = working_directory + "tmp/archive_files"
    shutil.make_archive(destination_path, "zip", source_path)

    # Remove the temporary "zip_archive" directory
    shutil.rmtree(dir_path)
    return


def place_archive_to_location(**kwargs):
    """
    Move the archive directory to the specified location.
    """
    archive_directory = kwargs["dag_run"].conf["archive_directory"]
    dir_path = working_directory + archive_directory
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    shutil.move(
        working_directory + "tmp/archive_files.zip", dir_path + "/" + "data_files.zip"
    )

    return
