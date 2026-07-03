#!/usr/bin/python3
import datetime
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

BASE_PATH = "/deepnote-toolkit/"
DEFAULT_DOWNLOAD_METHOD = "s3"
OCI_ZSTD_DOWNLOAD_METHOD = "oci-zstd"
DEFAULT_TOOLKIT_BUNDLE_OCI_REPOSITORY = "docker.io/deepnote/toolkit-bundle"


def build_toolkit_bundle_ref(
    release_name: str,
    python_version: str,
    oci_repository: str = DEFAULT_TOOLKIT_BUNDLE_OCI_REPOSITORY,
) -> str:
    return f"{oci_repository}:{release_name}-python{python_version}-tar-zst"


def _read_stderr(process: subprocess.Popen) -> bytes:
    if process.stderr is None:
        return b""
    return process.stderr.read()


def download_dependency_from_s3(
    release_name: str,
    python_version: str,
    toolkit_index_bucket_name: str,
    version_path: str,
) -> None:
    s3_path = f"s3://{toolkit_index_bucket_name}/deepnote-toolkit/{release_name}/python{python_version}.tar"
    print(f"{datetime.datetime.now()}: Downloading {release_name} {s3_path}")

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
    _, tar_process_stderr = tar_process.communicate()
    aws_process_returncode = aws_process.wait()

    if aws_process_returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (aws s3 command failed): aws stderr: {_read_stderr(aws_process)}, tar stderr: {tar_process_stderr}"
        )
    if tar_process.returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (tar command failed): {tar_process_stderr}"
        )

    print(
        f"{datetime.datetime.now()}: Done downloading {release_name} {s3_path} and extracting to {version_path}"
    )


def download_dependency_from_oci_zstd(
    release_name: str,
    python_version: str,
    oci_repository: str,
    version_path: str,
) -> None:
    artifact_file = f"python{python_version}.tar.zst"
    artifact_ref = build_toolkit_bundle_ref(
        release_name, python_version, oci_repository
    )
    print(f"{datetime.datetime.now()}: Downloading {release_name} {artifact_ref}")

    regctl_process = subprocess.Popen(
        ["regctl", "artifact", "get", "--file", artifact_file, artifact_ref],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    zstd_process = subprocess.Popen(
        ["zstd", "-dc"],
        stdin=regctl_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    regctl_process.stdout.close()
    tar_process = subprocess.Popen(
        ["tar", "-xf", "-", "-C", version_path],
        stdin=zstd_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    zstd_process.stdout.close()

    _, tar_process_stderr = tar_process.communicate()
    zstd_process_returncode = zstd_process.wait()
    regctl_process_returncode = regctl_process.wait()

    if regctl_process_returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (regctl command failed): regctl stderr: {_read_stderr(regctl_process)}, zstd stderr: {_read_stderr(zstd_process)}, tar stderr: {tar_process_stderr}"
        )
    if zstd_process_returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (zstd command failed): zstd stderr: {_read_stderr(zstd_process)}, tar stderr: {tar_process_stderr}"
        )
    if tar_process.returncode != 0:
        raise Exception(
            f"Error downloading {release_name} (tar command failed): {tar_process_stderr}"
        )

    print(
        f"{datetime.datetime.now()}: Done downloading {release_name} {artifact_ref} and extracting to {version_path}"
    )


def download_dependency(
    release_name: str,
    python_version: str,
    toolkit_index_bucket_name: str,
    download_method: str = DEFAULT_DOWNLOAD_METHOD,
    oci_repository: str = DEFAULT_TOOLKIT_BUNDLE_OCI_REPOSITORY,
):
    """Download the dependencies for the given Python version and release name."""

    version_path = os.path.join(BASE_PATH, release_name, f"python{python_version}")
    done_file = os.path.join(version_path, f"{python_version}-done")

    if Path(done_file).is_file():
        print(
            f"{datetime.datetime.now()}: {release_name} python{python_version} already cached, skipping download"
        )
        return

    os.makedirs(version_path, exist_ok=True)

    if download_method == DEFAULT_DOWNLOAD_METHOD:
        download_dependency_from_s3(
            release_name,
            python_version,
            toolkit_index_bucket_name,
            version_path,
        )
    elif download_method == OCI_ZSTD_DOWNLOAD_METHOD:
        download_dependency_from_oci_zstd(
            release_name,
            python_version,
            oci_repository,
            version_path,
        )
    else:
        raise ValueError(
            f"Unsupported TOOLKIT_DOWNLOAD_METHOD={download_method!r}; expected one of: {DEFAULT_DOWNLOAD_METHOD}, {OCI_ZSTD_DOWNLOAD_METHOD}"
        )

    # Create the "done" file
    Path(done_file).touch()


def submit_downloading(
    python_versions: List[str],
    release_name: str,
    toolkit_index_bucket_name: str,
    download_method: str = DEFAULT_DOWNLOAD_METHOD,
    oci_repository: str = DEFAULT_TOOLKIT_BUNDLE_OCI_REPOSITORY,
):
    """Download the dependencies for the given Python versions and release name."""

    with ThreadPoolExecutor(max_workers=len(python_versions)) as executor:
        futures = [
            executor.submit(
                download_dependency,
                release_name,
                python_version,
                toolkit_index_bucket_name,
                download_method,
                oci_repository,
            )
            for python_version in python_versions
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Download failed with: {exc}")
                sys.exit(1)


def cleanup_old_versions(
    base_path: str, current_release_name: str, versions_to_keep: int = 2
) -> None:
    """Remove old toolkit versions, keeping only the most recent ones.

    Sorts existing version directories by modification time and removes all
    but the ``versions_to_keep`` newest. The directory for
    ``current_release_name`` (about to be downloaded) is excluded from
    removal so the subsequent download can populate it cleanly.

    After cleanup there will be at most ``versions_to_keep`` old versions on
    disk. Once the new version finishes downloading the total will be
    ``versions_to_keep + 1``.
    """

    root = Path(base_path)

    if not root.is_dir():
        return

    if versions_to_keep < 0:
        raise ValueError(
            f"versions_to_keep must be non-negative, got {versions_to_keep}"
        )

    version_dirs = [
        entry
        for entry in root.iterdir()
        if entry.is_dir() and entry.name != current_release_name
    ]

    if len(version_dirs) <= versions_to_keep:
        return

    # Newest first
    version_dirs.sort(key=lambda e: e.stat().st_mtime, reverse=True)

    for entry in version_dirs[versions_to_keep:]:
        print(f"{datetime.datetime.now()}: Removing old toolkit version: {entry}")
        try:
            shutil.rmtree(entry)
        except OSError as exc:
            print(
                f"{datetime.datetime.now()}: Warning: failed to remove {entry}: {exc}"
            )


def main():
    """Main function to download the toolkit dependencies for the given Python versions."""

    start_time = datetime.datetime.now()
    print("Start time:", start_time)

    python_versions_env = os.getenv("PYTHON_VERSIONS")
    python_versions = [version.strip() for version in python_versions_env.split(",")]
    release_name = os.getenv("RELEASE_NAME")
    if not release_name:
        print("Error: RELEASE_NAME environment variable is not set")
        sys.exit(1)

    toolkit_index_bucket_name = os.getenv("TOOLKIT_INDEX_BUCKET_NAME")
    download_method = os.getenv("TOOLKIT_DOWNLOAD_METHOD", DEFAULT_DOWNLOAD_METHOD)
    oci_repository = os.getenv(
        "TOOLKIT_BUNDLE_OCI_REPOSITORY", DEFAULT_TOOLKIT_BUNDLE_OCI_REPOSITORY
    )

    cleanup_old_versions(BASE_PATH, release_name)
    submit_downloading(
        python_versions,
        release_name,
        toolkit_index_bucket_name,
        download_method,
        oci_repository,
    )

    end_time = datetime.datetime.now()
    print("End time:", end_time)

    # Calculate and print the elapsed time in seconds
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    sys.exit(0)


if __name__ == "__main__":
    main()
