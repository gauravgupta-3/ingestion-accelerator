from prefect import flow
from pathlib import Path

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/gauravgupta-3/ingestion-accelerator.git"

# This helper function reads the requirements.txt file.
def read_requirements(file_path="requirements.txt"):
    """Read and parse requirements.txt file"""
    requirements = Path(file_path).read_text().splitlines()
    # Filter out empty lines and comments.
    return [req.strip() for req in requirements if req.strip() and not req.startswith('#')]


if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="extract_rdbms.py:extract_rdbms", # Specific flow to run
    ).deploy(
        name="extract-rdbms-deployment",
        work_pool_name="my-managed-pool",
        job_variables={"pip_packages": read_requirements()},
        cron="0 * * * *",  # Run every hour
    )