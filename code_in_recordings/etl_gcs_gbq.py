# extract data from gcs
# transform - basic data cleaning example - fill passenger count nans with 0s
# load data into gbq

# pip install pandas-gbq pyarrow prefect-gcp['cloud_storage']
# pyarrow is larger, but can compress better than fastparquet

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from google.oauth2 import service_account

gcp_credentials_block = GcpCredentials.load("de-zoom-auth")


@task()
def transform(
    path: str,
) -> pd.DataFrame:  # simplified data cleaning example
    df_raw = pd.read_parquet(path)
    print(f'pre: missing passenger counts: {df_raw["passenger_count"].isna().sum()}')
    df_raw["passenger_count"] = df_raw["passenger_count"].fillna(0)
    print(f'post: missing passenger counts: {df_raw["passenger_count"].isna().sum()}')
    return df_raw


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    try:
        df.to_gbq(
            destination_table="prefect-sbx-community-eng.dezoomcamp.rides",
            project_id="prefect-sbx-community-eng",
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
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    # will get a file and write it out, no return

    # transform
    df = transform(gcs_path)  # same as local path

    # load
    write_bq(df)


if __name__ == "__main__":
    etl()
