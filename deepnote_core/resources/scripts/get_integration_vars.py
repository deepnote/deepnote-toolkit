#!/usr/bin/env python
"""
This script retrieves and sets environment variables for mounted integrations.
Dont need any dependencies, just python.

Usage:
    source <(/deepnote-configs/scripts/get_integration_vars.py)
"""

import json
import shlex
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


def fetch_with_retry(
    url: str, max_retries: int = 6, initial_delay: float = 0.1
) -> dict:
    """
    Fetch data from URL with exponential backoff retry mechanism.

    Args:
        url: The URL to fetch data from
        max_retries: Maximum number of retry attempts (default: 6)
        initial_delay: Initial delay in seconds before first retry (default: 0.1)

    Returns:
        dict: Decoded JSON response data

    Raises:
        URLError: If all retry attempts fail
    """
    for attempt in range(max_retries):
        try:
            # Attempt to fetch and parse the response
            request = Request(url)
            with urlopen(request) as response:
                return json.loads(response.read().decode())

        except URLError:
            # On last attempt, raise the error
            if attempt == max_retries - 1:
                raise
            # Calculate delay with exponential backoff
            delay = initial_delay * (2**attempt)
            time.sleep(delay)
    raise AssertionError("Unreachable: all retries should have raised URLError")


def main():
    """
    Main function to retrieve integration environment variables and print them
    in a format suitable for shell export commands.
    """
    # API endpoint for fetching environment variables
    url = "http://localhost:19456/userpod-api/integrations/environment-variables"

    try:
        # Fetch data with retry mechanism
        data = fetch_with_retry(url)

        # Print environment variables in export format
        for variable in data:
            print(f"export {variable['name']}={shlex.quote(variable['value'])}")

    except Exception as e:
        print(
            f"Error: Failed to fetch integration environment variables after retries: {e}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
