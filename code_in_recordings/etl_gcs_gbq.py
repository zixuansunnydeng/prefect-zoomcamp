# extract data from gcs
# transform - basic data cleaning example - fill passenger count nans with 0s
# load data into gbq

# pip install pandas-gbq pyarrow prefect-gcp['cloud_storage']
# pyarrow is larger, but can compress better than fastparquet

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from google.oauth2 import service_account

gcp_credentials_block = GcpCredentials.load("de-zoom-auth")


@task()
def extract(color: str) -> Path:
    """Download parquet file from GCS"""
    path = Path(f"{color}/{color}_2022_09.parquet")

    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.get_directory(from_path=path, local_path="./")
    # will get a file and write it out, no return value
    return path


@task()
def transform(path: Path) -> pd.DataFrame:
    """Simplified data cleaning example"""
    df_raw = pd.read_parquet(path)
    print(f'pre: missing passenger counts: {df_raw["passenger_count"].isna().sum()}')
    df_raw["passenger_count"] = df_raw["passenger_count"].fillna(0)
    print(f'post: missing passenger counts: {df_raw["passenger_count"].isna().sum()}')
    return df_raw


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery table"""
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
    return


@task()
def cleanup(path: Path) -> None:
    """delete the file locally after use"""
    path.unlink(missing_ok=True)
    return


@flow()
def etl():
    """The main ETL flow"""
    color = "yellow"  # taxi color
    path = extract(color)
    df = transform(path)
    write_bq(df)
    cleanup(path)
    return


if __name__ == "__main__":
    etl()
