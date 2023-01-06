from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    service_account_file=".././de-prefect-sbx-community-eng-934851484082.json",
)
credentials_block.save("zoomcamp-gcp-creds-block", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoomcamp-gcp-creds-block"),
    bucket="prefect-de-zoomcamp",  # insert your  GCS bucket name
)

bucket_block.save("zoomcamp-gcs-bucket-block", overwrite=True)
