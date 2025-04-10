from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/gauravgupta-3/ingestion-accelerator.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="extract_rdbms.py:extract_rdbms", # Specific flow to run
    ).deploy(
        name="extract-rdbms-deployment",
        work_pool_name="my-managed-pool",
        cron="0 * * * *",  # Run every hour
    )