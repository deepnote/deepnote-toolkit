#!/usr/bin/python3
import datetime
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

BASE_PATH = "/deepnote-toolkit/"


def download_dependency(
    release_name: str, python_version: str, toolkit_index_bucket_name: str
):
    """Download the dependencies for the given Python version and release name."""

    version_path = os.path.join(BASE_PATH, release_name, f"python{python_version}")
    done_file = os.path.join(version_path, f"{python_version}-done")

    # Create the version directory if it doesn't exist
    os.makedirs(version_path, exist_ok=True)

    s3_path = f"s3://{toolkit_index_bucket_name}/deepnote-toolkit/{release_name}/python{python_version}.tar"
    print(f"{datetime.datetime.now()}: Downloading {release_name} {s3_path}")

    # Use Popen to stream the data
    aws_process = subprocess.Popen(
        ["aws", "s3", "cp", "--no-sign-request", s3_path, "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_process = subprocess.Popen(
        ["tar", "-xf", "-", "-C", version_path],
        stdin=aws_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    aws_process.stdout.close()  # Allow aws_process to receive a SIGPIPE if tar_process exits.
    _, tar_process_stderr = (
        tar_process.communicate()
    )  # Wait for tar_process to complete
    aws_process_returncode = aws_process.wait()

    if aws_process_returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (aws s3 command failed): aws stderr: {aws_process.stderr.read()}, tar stderr: {tar_process_stderr}"
        )
    # Check for errors
    if tar_process.returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (tar command failed): {tar_process_stderr}"
        )

    print(
        f"{datetime.datetime.now()}: Done downloading {release_name} {s3_path} and extracting to {version_path}"
    )
    # Create the "done" file
    open(done_file, "a").close()


def submit_downloading(
    python_versions: List[str], release_name: str, toolkit_index_bucket_name: str
):
    """Download the dependencies for the given Python versions and release name."""

    with ThreadPoolExecutor(max_workers=len(python_versions)) as executor:
        futures = [
            executor.submit(
                download_dependency,
                release_name,
                python_version,
                toolkit_index_bucket_name,
            )
            for python_version in python_versions
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Download failed with: {exc}")
                sys.exit(1)


def main():
    """Main function to download the toolkit dependencies for the given Python versions."""

    start_time = datetime.datetime.now()
    print("Start time:", start_time)

    python_versions_env = os.getenv("PYTHON_VERSIONS")
    python_versions = [version.strip() for version in python_versions_env.split(",")]
    release_name = os.getenv("RELEASE_NAME")
    toolkit_index_bucket_name = os.getenv("TOOLKIT_INDEX_BUCKET_NAME")

    submit_downloading(python_versions, release_name, toolkit_index_bucket_name)

    end_time = datetime.datetime.now()
    print("End time:", end_time)

    # Calculate and print the elapsed time in seconds
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    sys.exit(0)


if __name__ == "__main__":
    main()
