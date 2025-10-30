""" Generate a constraints.txt file from installed packages. """

import argparse
from importlib.metadata import PackageNotFoundError, distributions, requires

from packaging import markers
from packaging.requirements import Requirement


def list_installed_packages() -> list:
    """
    List all installed packages in the current environment.

    Returns:
        list: A list of package names.
    """
    installed_packages = distributions()

    return sorted([pkg.metadata["Name"] for pkg in installed_packages])


def write_constraints_file(constraints_file: str) -> None:
    """
    Write the requirements of all installed packages to a constraints.txt file
    Args:
        constraints_file (str): Path to the output constraints.txt file.
    """
    installed_packages = list_installed_packages()

    requirements = set()  # Use a set to automatically handle duplicates

    for package in installed_packages:
        try:
            package_requirements = requires(package)

            if not package_requirements:
                continue

            # List all requirements of the package
            for requirement_spec in package_requirements:
                # Parse the requirement string
                simple_req = Requirement(requirement_spec)

                if simple_req.marker:
                    marker = markers.Marker(str(simple_req.marker))
                    # Evaluate the marker to check python version or extras
                    # Filter out all unrelated
                    if not marker.evaluate():
                        continue

                # If version is not specified, skip the requirement
                if not simple_req.specifier:
                    continue

                print(
                    f"Package {package} requires {simple_req.name} with marker {simple_req.marker}"
                )
                requirements.add(f"{simple_req.name}{simple_req.specifier}")

        except PackageNotFoundError:
            print(f"Package not found: {package}")

    # Sort the requirements alphabetically
    sorted_requirements = sorted(requirements)

    # Write sorted requirements to the constraints file
    with open(constraints_file, "w", encoding="utf-8") as file:
        for requirement in sorted_requirements:
            file.write(f"{requirement}\n")


def main():
    """
    Main function to parse command-line arguments and generate the constraints file.
    """
    parser = argparse.ArgumentParser(
        description="Generate a constraints.txt file from installed packages."
    )
    parser.add_argument(
        "--output", required=True, help="Path to the output constraints.txt file"
    )

    args = parser.parse_args()
    write_constraints_file(args.output)


if __name__ == "__main__":
    main()
