# fetch data from web
# inspect
# write data locally
# Write to gcs bucket

import random
import pandas as pd  # install pandas and pyarrow, larger, but can compress better than fastparquet
from prefect import flow, task


@task(retries=3)
def extract() -> pd.DataFrame:  # later parametrize with month and year and color
    # read data into pandas dataframe

    # create artificial failure to show value of retries
    # if random.randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_parquet(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-09.parquet"
    )
    print(df.head(2))  # log_prints shows in terminal and ui
    print(f"columns: {df.columns}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame) -> str:
    color = "yellow"
    year = "2022"
    month = "09"
    path = f"{color}/{color}_{year}_{month}.parquet"
    df.to_parquet(
        path, compression="gzip"
    )  # default is snappy compression, not as much compression as gzip
    # pyarrow is used by default, if available. compresses better, so make sure available
    return path


@flow()
def el3() -> None:
    from prefect_gcp.cloud_storage import GcsBucket

    df = extract()
    path = write_local(df)
    color = "yellow"
    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.put_directory(local_path=f"{color}", to_path="yellow")


if __name__ == "__main__":
    el3()
