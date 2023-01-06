# Data Engineering Zoomcamp 2023 Week 2 
## Prefect

This repo contains Python code to accompany the videos that show how to use Prefect with Google Cloud Storage and BigQuery for ETL. Prefect helps you observe and orchestrate your dataflow

# Setup

## Clone the repo

Clone the repo locally.

## Install packages

In a conda environment, install all package dependencies with 

```bash
pip install -r requirements.txt
```
## Start the Prefect Orion server locally

Create another window and activate your conda environment. Start the Orion API server locally with 

```bash
prefect orion start
```

## Set up GCP 

- Log in to [GCP](https://cloud.google.com/)
- Create a Project
- Set up Cloud Storage
- Set up BigQuery
- Create a service account with the required policies to interact with both services

## Register the block types that come with prefect-gcp

`prefect block register -m prefect_gcp.cloud_storage`

## Create Prefect GCP blocks

Create a *GCP Credentials* block in the UI.

Paste your service account information from your JSON file into the *Service Account Info* block's field.

![img.png](images/img.png)

Create GCS Bucket block in UI 

Alternatively, create these blocks using code by following the template in the [blocks](./blocks/) folder

## Create flow code

Write your Python functions and add `@flow` and `@task` decorators.

## Create deployments

Create and apply your deployments.

## Run a deployment or create a schedule

Run a deployment ad hoc from the CLI or UI.

Or create a schedule from the UI or when you create your deployment.

## Start an agent

Make sure your agent set up to poll the work queue you created when you made your deployment (*default* if you didn't specify a work queue).

# Create GitHub flow code storage block

Add some collaboration capabilities. Create a GitHub repo, push your flow code there wit git, and create a GitHub storage block. Reference the block when you create a deployment.

# Optional: create infrastructure blocks

Use a Docker Container block to run your flow code in a Docker container.

# Optional: use Prefect Cloud server for added capabilties
Signup for free at https://app.prefect.cloud