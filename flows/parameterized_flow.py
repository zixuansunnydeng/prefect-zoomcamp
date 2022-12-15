from datetime import timedelta
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from google.oauth2 import service_account


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(color: str, date: str) -> Path:
    """Download trip data parquet file from GCS"""
    path = Path(f"{color}/{color}_{date}.parquet")

    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.get_directory(from_path=path, local_path="./")
    return path


@task()
def transform(path: Path) -> pd.DataFrame:
    """Simplified data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"] = df["passenger_count"].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(retries=3, retry_delay_seconds=60)
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
def etl(color: str, date: str):
    """Main ETL flow"""
    path = extract(color, date)
    df = transform(path)
    write_bq(df)
    cleanup(path)


@flow()
def parent_etl(color: str, dates: list):
    for date in dates:
        etl(color, date)


if __name__ == "__main__":
    parent_etl('yellow', ['2022_08', '2022_09', '2022_10'])

# Create the deployment file by running this CLI:
# prefect deployment build ./parameterized_flow.py:parent_etl -n parent_etl_flow -t zoomcamp -q local_queue -o deployment.yaml

# Update the parameters in the deployment.yaml
# parameters: {"color": "yellow", "dates": ['2022_08', '2022_09', '2022_10']}

# Apply the deployment to Prefect Cloud
# prefect deployment aply ./deployment.yaml
