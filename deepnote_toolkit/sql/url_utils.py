from urllib.parse import urlparse, urlunparse


def replace_user_pass_in_pg_url(pg_url, new_username, new_password):
    """
    Replaces the username and password in a PostgreSQL URL.

    Parameters:
    - pg_url (str): The original PostgreSQL URL.
    - new_username (str): The new username to use.
    - new_password (str): The new password to use.

    Returns:
    - str: The modified PostgreSQL URL with the new username and password.
    """

    parsed_url = urlparse(pg_url)

    new_netloc = f"{new_username}:{new_password}@{parsed_url.hostname}"

    if parsed_url.port:
        new_netloc += f":{parsed_url.port}"

    modified_url = parsed_url._replace(netloc=new_netloc)

    return urlunparse(modified_url)
