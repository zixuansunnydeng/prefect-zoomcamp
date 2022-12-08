# download taxi data -- Parquet to pandas 
# transform the taxi data
# load taxi data

# we do average fare amount for yellow -- they change it for average trip distance. - TODO edit trasnform

# take the flow for just one month  - taxi data ()
# demo subflow with all months -- year



"""
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:taxi_data -n yellow -q default -a
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:taxi_data -n yellow -q default -a --param table_name=green_tripdata
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:parent -n yellow -q default -a
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:parent -n yellow -q default -a --param table_name=green_tripdata
"""
from datetime import datetime
import pandas as pd
from prefect import task, flow, get_run_logger
from prefect.blocks.system import JSON
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task_input_hash

from prefect_utils import BigQueryPandas
from prefect_utils.tasks import get_files_to_process, extract, transform
from prefect.blocks.core import Block
from prefect_gcp.credentials import GcpCredentials


@task
def load(df: pd.DataFrame, file: str, tbl: str, if_exists: str = "append") -> None:
    logger = get_run_logger()
    block = BigQueryPandas.load("default")
    block.load_data(dataframe=df, table_name=tbl, if_exists=if_exists)
    ref = block.credentials.get_bigquery_client().get_table(tbl)
    logger.info(
        "Loaded %s to %s ✅ table now has %d rows and %s GB",
        file,
        tbl,
        ref.num_rows,
        ref.num_bytes / 1_000_000_000,
    )


MAIN_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


@task
def get_files_to_process(year: int = 2022, service_type: str = "yellow") -> List[str]:
    svc = f"{service_type}_tripdata_{year}"
    files = [f"{svc}-{str(i).zfill(2)}.parquet" for i in range(1, 13)]
    valid_files = []
    for file in files:
        try:
            status_code = urlopen(f"{MAIN_URL}{file}").getcode()
            if status_code == 200:
                valid_files.append(file)
        except HTTPError:
            pass
    return valid_files


@task(retries=3, retry_delay_seconds=60)
def extract(file_name: str) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        raw_df = pd.read_parquet(f"{MAIN_URL}{file_name}")
        logger.info("Extracted %s with %d rows", file_name, len(raw_df))
        return raw_df
    except HTTPError:
        logger.warning("File %s is not available in TLC Trip Record Data")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform(
    df: pd.DataFrame, file_name: str, service_type: str = "yellow"
) -> pd.DataFrame:
    df["file"] = file_name
    df[service_type] = service_type
    df["ingested"] = datetime.utcnow().isoformat()
    return df

@task()
def load_data(
    dataframe: pd.DataFrame,
    table_name: str,  # dataset.tablename
    chunksize: Optional[int] = 10_000,
    if_exists: str = "append"
    credentials
) -> None:
    dataframe.to_gbq(
        destination_table=table_name,
        chunksize=chunksize,
        if_exists=if_exists,
        credentials=credentials.get_credentials_from_service_account()
    )

@task()
def create_dataset_if_not_exists(dataset: str, credentials) -> None:
    client = credentials.get_bigquery_client()
    try:
        client.get_dataset(dataset)
    except NotFound:
        client.create_dataset(dataset)

@task
def update_pocessed_files(
    df: pd.DataFrame,
    file: str,
    tbl: str,
    service_type: str = "yellow",
    reset_block_value: bool = False,
) -> None:
    try:
        block = JSON.load(service_type)
    except ValueError:
        block = JSON(value={})
    files_processed = {} if reset_block_value else block.value
    now = datetime.utcnow().isoformat()
    files_processed[file] = dict(table=tbl, nrows=len(df), ingested_at=now)
    block = JSON(value=files_processed)
    block.save(service_type, overwrite=True)


@task
def check_if_processed(file: str, service_type: str = "yellow") -> bool:
    try:
        block = JSON.load(service_type)
    except ValueError:
        block = JSON(value={})
    logger = get_run_logger()
    files_processed = block.value
    exists = file in files_processed.keys()
    if exists:
        logger.info("File %s already ingested: %s", file, files_processed[file])
    return exists


@flow(task_runner=SequentialTaskRunner())
def taxi_etl(
    dataset: str = "trips_data",
    table_name: str = "yellow_tripdata",
    year: int = 2022,
    service_type: str = "yellow",
    if_exists: str = "append",
):
    files = get_files_to_process(year, service_type)
    tbl = f"{dataset}.{table_name}"
    credentials=GcpCredentials.load("default"),
    create_dataset_if_not_exists(dataset, credentials) 
    for file in files:
        if (
            not check_if_processed.with_options(name=f"check_{file}")
            .submit(file, service_type)
            .result()
        ):
            df = extract.with_options(name=f"extract_{file}").submit(file)
            df = transform.with_options(name=f"transform_{file}").submit(
                df.result(), file
            )
            load.with_options(name=f"load_{file}").submit(
                df.result().head(100),
                file,
                tbl,
                if_exists=if_exists,  # TODO remove .head(100) to load full dataset
            )
            update_pocessed_files.with_options(name=f"update_block_{file}").submit(
                df.result(), file, tbl, service_type
            )

@flow()
def parent_flow(
    years: List[] = [2019, 2020, 2021, 2022]
):
    for x in years:
        taxi_etl(year=x)

if __name__ == "__main__":
    parent_flow()
