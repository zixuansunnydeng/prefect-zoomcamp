from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.oauth2 import service_account


@task()
def extract(color: str) -> Path:
    """Download trip data parquet file from GCS"""
    path = Path(f"{color}/{color}_2022_09.parquet")

    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.get_directory(from_path=path, local_path="./")
    return path


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
    gcp_credentials_block = GcpCredentials.load("de-zoom-auth")

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
def etl():
    """Main ETL flow"""
    color = "yellow"  # taxic color
    path = extract(color)
    df = transform(path)
    write_bq(df)
    cleanup(path)


if __name__ == "__main__":
    etl()
