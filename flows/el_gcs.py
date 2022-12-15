from random import randint
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def extract() -> pd.DataFrame:
    """Read data from web into pandas DataFrame and inspect"""
    # create an artificial failure to show value of retries
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_parquet(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-09.parquet"
    )
    print(df.head(2))
    print(f"columns: {df.columns}")
    print(f"rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame) -> Path:
    """Write out DataFrame to local parquet file"""
    color = "yellow"
    year = "2022"
    month = "09"
    path = Path(f"{color}/{color}_{year}_{month}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(color: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.put_directory(local_path=color, to_path=color)
    return


@task()
def cleanup(path: Path) -> None:
    """Delete the local file after use"""
    path.unlink()
    return


@flow()
def el() -> None:
    """The main extract and load function"""
    color = "yellow"  # taxi color
    df = extract()
    path = write_local(df)
    write_gcs(color)
    cleanup(path)
    return


if __name__ == "__main__":
    el()
