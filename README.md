# Data Engineering Zoomcamp 2023 Week 2 
## Prefect

This repo contains Python code to accompany the videos that show how to use Prefect with BigQuery for ETL.

# Setup

## Install packages

In a conda environment, install Prefect 2.7.1.

```bash
pip install prefect==2.7.1
```

Or install all package dependencies with 

```bash
pip install -r requirements.txt
```

## Authenticate with Prefect Cloud

Create an API key in your Prefect Cloud workspace. Run the provided command in the terminal


# Set up GCP 

- Log in 
- Create Project
- Set up BigQuery

---

## Create Prefect GCP credentials block

Create a `GcpCredentials` block via Python code or the UI.
Paste your service account information from your JSON file into the `service_account_info` block's field.

![img.png](images/img.png)

# Create flow code
Iterate on it.

# Create deployment

Create and apply your deployment

# Run a deployment or create a schedule

Run a deployment ad hoc from the CLI or UI.

Or create a schedule from the UI or when you create your deployment.

# Start an agent

Make sure it set up to poll the work queue you created when you made your deployment (*default* if you didn't specify a work queue).

# Create GitHub flow code storage block

Add some collaboration capabilities. Create a GitHub repo, push your flow code there, and create a GitHub storage block. Reference the block when you create a deployment.

# Optional: create infrastructure blocks

Use a Docker Container block to run your flow code in Docker container.

