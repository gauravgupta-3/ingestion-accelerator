
from pathlib import Path

def read_requirements(file_path="C:/Gaurav/Project/Accelerator/prefect/ingestion/deployment/requirements.txt"):
    """Read and parse requirements.txt file"""
    with open(file_path) as f:
        lines = f.readlines()
    # Filter out empty lines and comments.
    print("Path",Path(file_path))
    print("lines",lines)
    #a= [req.strip() for req in requirements if req.strip() and not req.startswith('#')]
    #print("a",a)
read_requirements()