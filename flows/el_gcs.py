from random import randint
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

color = "yellow"
dataset_file = f"{color}_tripdata_2021-01"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}.csv.gz"


@task(log_prints=True, retries=3)
def extract() -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    # create an artificial failure to show value of retries
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    # fix dtype issues - learned of from exploring in notebook Jupyter Lab
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": 1, "N": 0})
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame) -> Path:
    """Write DataFrame to local parquet file"""
    path = Path(f"../data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-zoom")
    gcs_block.put_directory(
        local_path=f"../data/{color}", to_path=color, ignore_file=gcs_ignore
    )
    return


@flow()
def el() -> None:
    """The main extract and load function"""
    df = extract()
    df_clean = clean(df)
    path = write_local(df_clean)
    write_gcs(path)
    return


if __name__ == "__main__":
    el()
