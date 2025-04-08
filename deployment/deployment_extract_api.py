from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/gauravgupta-3/ingestion-accelerator.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="extract_api.py:extract_api", # Specific flow to run
    ).deploy(
        name="extract-api-deployment",
        work_pool_name="my-work-pool",
        cron="0 * * * *",  # Run every hour
    )