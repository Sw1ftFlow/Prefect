from prefect import flow
from prefect_databricks import DatabricksCredentials
from prefect_databricks.flows import jobs_runs_submit_and_wait_for_completion

@flow(log_prints=True)
async def trigger_dlt_pipeline(pipeline_id: str):
    """
    Triggers a Databricks run to update a Delta Live Tables pipeline.
    """
    databricks_credentials = DatabricksCredentials.load("databricks-dlt-connection")

    job_configuration = {
        "run_name": f"Prefect DLT Run for {pipeline_id}",
        "pipeline_task": {
            "pipeline_id": pipeline_id,
            "full_refresh": True
        },
        "timeout_seconds": 3600
    }

    print(f"Submitting run for DLT Pipeline ID: {pipeline_id}")

    run = await jobs_runs_submit_and_wait_for_completion(
        databricks_credentials=databricks_credentials,
        job_configuration=job_configuration
    )

    print(f"Databricks Run ID: {run.run_id}")

if __name__ == "__main__":
    print("This file is intended for deployment, please use the 'prefect deploy' command.")
