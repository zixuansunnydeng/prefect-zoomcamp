# extract data from gcs
# transform by removing nulls - basic data clearning example
# write locally
# load data into gbq

# pip install -U pandas-gbq

import pandas as pd  # install pandas and pyarrow, larger, but can compress better than fastparquet
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_file  # , bigquery_load_cloud_storage
from google.oauth2 import service_account

gcp_credentials_block = GcpCredentials.load("de-zoom-auth")


@task()
def transform(
    path: str,
) -> pd.DataFrame:  # transform by removing nulls - simplifieds data clearning example
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


# I don't have a check-if-processed that uses a json block like Anna had -
# I think maybe make that a stretch goal for homework to add?

# @task() # don't use if writing cirrectly form pandas to gbq
# def write_local(df: pd.DataFrame) -> str:  # write transformed file
#     color = "yellow"
#     path = f"{color}/cleaned_{color}_2022_09.parquet"
#     df.to_parquet(path, compression="gzip")
#     return path


@flow()
def etl():
    color = "yellow"

    # extract
    gcs_path = f"{color}/{color}_2022_09.parquet"

    gcs_block = GcsBucket.load("gcs-best")
    gcs_block.get_directory(  # also will just get a file and writes it out, no return
        from_path=gcs_path,
        local_path="./"
        # "https://storage.cloud.google.com/prefect-de-zoomcamp/yellow/yellow_2022_09.parquet"
    )

    df = transform(gcs_path)  # same as local path
    write_bq(df)
    # path = write_local(df)  # don't write locally if going write from pandas
    # print(path)

    # load data into gbq - with bigquery table created in UI
    # fails with
    # 400 Provided Schema does not match Table prefect-sbx-community-eng:dezoomcamp.rides.
    # Cannot add fields (field: string_field_0)
    # would need to tell to append if exists presumably somehow, too

    # result = bigquery_load_file(
    #     path=f"{path}",
    #     dataset="dezoomcamp",
    #     table="rides",
    #     gcp_credentials=gcp_credentials_block,
    #     # schema # optional for creating table # Optional[List[google.cloud.bigquery.schema.SchemaField]]
    # )
    # print(result)


if __name__ == "__main__":
    etl()
