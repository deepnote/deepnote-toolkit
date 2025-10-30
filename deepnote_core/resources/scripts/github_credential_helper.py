#!/usr/bin/env python

"""
Standalone Git Credential Helper Script

This script acts as a Git credential helper, as described in the Git documentation:
https://git-scm.com/docs/git-credential. It is designed for use in user PODs to
facilitate authentication with GitHub repositories over HTTPS.

When configured, Git will invoke this script whenever a user attempts to access a
GitHub repository. If Deepnote possesses a token for the specified repository, the
script will provide it. Otherwise, it returns an empty string, allowing the next
credential helper in the chain to be used.

This script is standalone and does not require any external dependencies beyond the
Python standard library. Additionally, it can be executed from the terminal to fetch
integration variables directly into the terminal environment.

"""

import sys
import urllib.parse
import urllib.request
from typing import Dict, Optional, TextIO, Tuple
from urllib.error import URLError


def parse_input(inp: str) -> Dict[str, str]:
    """
    Parse the input string from stdin into a dictionary of key-value pairs.

    Args:
        inp (str): The input string containing key-value pairs separated by '='.

    Returns:
        Dict[str, str]: A dictionary with keys and values parsed from the input string.
    """
    ret = {}
    for line in inp.splitlines():
        name, value = line.split("=")
        ret[name] = value
    return ret


def fetch_key(owner: str, repo: str) -> Optional[str]:
    """
    Retrieve the token for a specific GitHub repository from a local service.

    Args:
        owner (str): The owner of the repository.
        repo (str): The name of the repository.

    Returns:
        Optional[str]: The token if available, otherwise None.
    """
    token_service_url = "http://localhost:19456/userpod-api/git-token"
    query_params = urllib.parse.urlencode({"owner": owner, "repo": repo})
    full_url = f"{token_service_url}?{query_params}"

    try:
        with urllib.request.urlopen(full_url, timeout=5) as response:
            if response.status == 200:
                return response.read().decode("utf-8")
    except URLError:
        return None
    return None


def parse_repo_path(repo_path: str) -> Tuple[str, str]:
    """
    Extract the owner and repository name from the repository path.

    Args:
        repo_path (str): The repository path in the format 'owner/repo'.

    Returns:
        Tuple[str, str]: A tuple containing the owner and repository name.
    """
    owner, repo = repo_path.split("/")
    if repo.endswith(".git"):
        repo = repo[: -len(".git")]
    return owner, repo


def generate_credentials(parsed_input: Dict[str, str]) -> Optional[Dict[str, str]]:
    """
    Generate a credentials dictionary if the input is valid.

    Args:
        parsed_input (Dict[str, str]): The parsed input dictionary containing
        'host', 'path', and 'protocol'.

    Returns:
        Optional[Dict[str, str]]: A dictionary containing credentials if successful, otherwise None.
    """
    host = parsed_input.get("host")
    path = parsed_input.get("path")
    protocol = parsed_input.get("protocol")

    if (
        None in (host, path, protocol)
        or "" in (host, path, protocol)
        or host != "github.com"
        or protocol != "https"
    ):
        return None

    if path is None:
        raise ValueError("Unable to generate credentials without repository path")
    owner, repo = parse_repo_path(path)
    key = fetch_key(owner, repo)

    if key is None:
        return None
    return {**parsed_input, "username": "x-access-token", "password": key}


def main(output: TextIO = sys.stdout) -> None:
    """
    Main function to read input, generate credentials, and print them to the output.

    Args:
        output: The output stream to print the credentials. Defaults to sys.stdout.
    """
    parsed_input = parse_input(sys.stdin.read())
    credentials = generate_credentials(parsed_input)
    if credentials is not None:
        credentials_string = "\n".join(
            f"{item[0]}={item[1]}" for item in credentials.items()
        )
        print(credentials_string, file=output)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Github credential helper exited due to {e}", file=sys.stderr)
        sys.exit(1)
