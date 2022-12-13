# Taxi dataset homework

1. Using Prefect flows and tasks with the *green* instead of *yellow* taxi dataset, injest the data into your data lake in Google Cloud Storage, transform it, and put put it into BigQuery.
1. Create a deployment for the green taxi workflow(s) you created.
1. Schedule the deployment to run every Sunday at 5pm UTC
1. Run a parametrized deployment for the green taxi dataset for the all the data in the year 2021. 
1. Use a GitHub storage block with your flow code on GitHub.
1. _Bonus:_ Create a deployment with your flow code on GCS in a storage block.
1. _Bonus:_ Run your code in Docker with a DockerContainer infrastructure block.
1. _Bonus:_ Use a a GCP Cloud Run Job infrastructure block for remote execution of flow runs.
1. _Bonus:_ Run your agent in Google Cloud Engine.
1. _Double Bonus:_ Build a CI/CD setup for your deployments with GitHub Actions.