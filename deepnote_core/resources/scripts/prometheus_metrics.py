"""Script to report data from the environment to Prometheus."""

import subprocess
import time

# pylint: disable=import-error
from prometheus_client import Gauge, start_http_server


def run_pip_check():
    """Run the pip check command and return the number of issues and the output."""
    try:
        result = subprocess.run(
            ["pip", "check"], capture_output=True, text=True, check=True
        )
        # If pip check completes without error, there are no issues.
        return 0, result.stdout
    except subprocess.CalledProcessError as err:
        # If pip check returns a non-zero exit code, count the number of lines in stderr.
        return len(err.stderr.strip().split("\n")), err.stderr


def main():
    """Main function to set up Prometheus metrics and run pip check periodically."""
    # Create a Gauge metric to track the number of issues found by pip check.
    pip_check_issues = Gauge("pip_check_issues", "Number of issues found by pip check")

    # Start the Prometheus HTTP server on port 9103.
    start_http_server(9103)

    while True:
        # Run pip check and update the metric.
        issues_count, output = run_pip_check()
        pip_check_issues.set(issues_count)

        # Print the result to standard output.
        print(f"Pip check output:\n{output}")
        print(f"Updated pip_check_issues metric: {issues_count}")

        # Wait for an hour before running again.
        time.sleep(3600)


if __name__ == "__main__":
    main()
