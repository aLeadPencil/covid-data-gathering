import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from selenium import webdriver

from data_cleaning_constants import *
from data_cleaning_functions import *
from credentials import *

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDS
driver = webdriver.Chrome()

if __name__ == "__main__":
    client = bigquery.Client()
    extracted_dates, raw_urls = find_all_raw_urls(GITHUB_URL, BASE_URL, driver)
    fill_database(extracted_dates, raw_urls, client, TABLE_ID)
