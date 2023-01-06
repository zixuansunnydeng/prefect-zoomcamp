from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoomcamp-gcp-creds-block"),
    bucket="prefect-de-zoomcamp",
)

bucket_block.save("zoomcamp-gcs-bucket-block", overwrite=True)
