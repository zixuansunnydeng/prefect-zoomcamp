# extract data from gcs
# transform by removing nulls - basic data clearning example
# load data into gbq

# pip install pandas-gbq pyarrow prefect-gcp['cloud_storage']
# pyarrow larger, but can compress better than fastparquet

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_file
from google.oauth2 import service_account

gcp_credentials_block = GcpCredentials.load("de-zoom-auth")


@task()
def transform(
    path: str,
) -> pd.DataFrame:  # simplified data cleaning example
    df_raw = pd.read_parquet(path)
    print(f'pre: missing passenger count rows: {df_raw["passenger_count"].isna()}')
    df_raw["passenger_count"] = df_raw["passenger_count"].fillna(0)  # fill with 0
    print(f'post: missing passenger count rows: {df_raw["passenger_count"].isna()}')
    return df_raw


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    try:
        df.to_gbq(
            destination_table="prefect-sbx-community-eng.dezoomcamp.rides",
            project_id="prefect-sbx-community-eng",  # dataset id ok?
            credentials=service_account.Credentials.from_service_account_info(
                gcp_credentials_block.service_account_info
            ),
            chunksize=500_000,
            if_exists="append",
            progress_bar=True,
        )
    except Exception as e:
        print(f"Exception {e} - oops, did not load")


@flow()
def etl():
    color = "yellow"

    # extract
    gcs_path = f"{color}/{color}_2022_09.parquet"

    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.get_directory(  # also will just get a file and writes it out, no return
        from_path=gcs_path, local_path="./"
    )

    df = transform(gcs_path)  # same as local path
    write_bq(df)


if __name__ == "__main__":
    etl()
