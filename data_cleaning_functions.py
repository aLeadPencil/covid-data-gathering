import pandas as pd
import pandas_gbq
from bs4 import BeautifulSoup
from google.cloud.exceptions import NotFound
from data_cleaning_constants import EXCLUDED_STATES, DROPPED_COLUMNS, US_STATE_ABBREV


def find_all_raw_urls(github_url, base_url, driver):
    """
    Extract all links from a github repository page
    Extract the date of each csv file which will be used later to name the file when saving it

    Parameters:
    -----------
    github_url: string of github repository url
    base_url: string of root raw_url
    driver: selenium driver object

    Returns:
    extracted_dates: list of all available dates
    raw_urls: list of all available raw_urls
    """

    # Set up connection to the github url
    driver.get(github_url)

    # Create empty lists to store necessary information
    all_links = []
    extracted_dates = []
    raw_urls = []

    # Obtain all links and filter out duplicates
    github_html = driver.page_source
    soup = BeautifulSoup(github_html, "lxml")
    driver.quit()

    for link in soup.find_all("a"):
        href_link = link.get("href")
        all_links.append(href_link)

    all_links = [link for link in all_links if link and ".csv" in link]
    all_links = list(set(all_links))

    # Obtain all dates where it's the first of the month
    for link in all_links:
        date = link[-14:-4]
        extracted_dates.append(date)
    extracted_dates = [date for date in extracted_dates if date[3:5] == "01"]

    # Obtain all raw urls
    for date in extracted_dates:
        raw_url = base_url + date + ".csv"
        raw_urls.append(raw_url)

    return extracted_dates, raw_urls


def fill_database(extracted_dates, raw_urls, client, table_id):
    """
    Read a bigquery table into memory
    If the table exists then check for missing records and fill them in
    If the table does not exist then fill the table with all available records

    Parameters:
    -----------
    extracted_dates: list of all available dates
    raw_urls: list of all available raw_urls
    client: bigquery client connection
    table_id: table id where data will be saved to

    Returns:
    None
    """

    # If table exists, fill in any missing records
    try:
        client.get_table(table_id)
        df = pandas_gbq.read_gbq(table_id)
        existing_dates = df["Date"].unique().tolist()
        existing_row_count = df.shape[0]

        for idx, _ in enumerate(extracted_dates):
            if extracted_dates[idx] not in existing_dates:
                tmp_df = pd.read_csv(raw_urls[idx])
                tmp_df = tmp_df[
                    ~tmp_df["Province_State"].isin(EXCLUDED_STATES)
                ].reset_index(drop=True)
                tmp_df = tmp_df.drop(DROPPED_COLUMNS, axis=1, errors="ignore")
                tmp_df["Date"] = extracted_dates[idx]

                df = pd.concat([df, tmp_df], ignore_index=True).drop_duplicates(
                    keep=False
                )

        df = df.iloc[existing_row_count:]
        df["State_Code"] = df["Province_State"].map(US_STATE_ABBREV)
        pandas_gbq.to_gbq(df, table_id, if_exists="append")
        print("Missing Records Filled")

    # If table doesn't exist, create table with all available records
    except NotFound:
        df = pd.DataFrame()

        for idx, _ in enumerate(raw_urls):
            tmp_df = pd.read_csv(raw_urls[idx])
            tmp_df = tmp_df[
                ~tmp_df["Province_State"].isin(EXCLUDED_STATES)
            ].reset_index(drop=True)
            tmp_df = tmp_df.drop(DROPPED_COLUMNS, axis=1, errors="ignore")
            tmp_df["Date"] = extracted_dates[idx]

            df = pd.concat([df, tmp_df], ignore_index=True).drop_duplicates(keep=False)

        df["State_Code"] = df["Province_State"].map(US_STATE_ABBREV)
        pandas_gbq.to_gbq(df, table_id)
        print("Empty Table Filled")
