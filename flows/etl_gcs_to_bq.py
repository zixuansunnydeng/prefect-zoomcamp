from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.oauth2 import service_account


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data parquet file from GCS"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    # TODO # change with new block
    gcs_block = GcsBucket.load("gcs-zoom")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Simple data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"] = df["passenger_count"].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    # load credentials block
    gcp_credentials_block = GcpCredentials.load("gcp-zoomcamp")

    df.to_gbq(
        destination_table="prefect-sbx-community-eng.dezoomcamp.rides",
        project_id="prefect-sbx-community-eng",
        credentials=service_account.Credentials.from_service_account_info(
            gcp_credentials_block.service_account_info
        ),
        chunksize=500_000,
        if_exists="append",
    )
    return


@task()
def cleanup(path: Path) -> None:
    """Delete local file after use"""
    path.unlink()
    return


@flow()
def etl_gcs_bq():
    """Main ETL flow to load data into the warehouse"""
    color = "yellow"  # taxi color
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    cleanup(path)


if __name__ == "__main__":
    etl_gcs_bq()
