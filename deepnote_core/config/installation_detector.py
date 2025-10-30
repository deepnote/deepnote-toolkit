from enum import Enum
from importlib.resources import files
from pathlib import Path


class InstallMethod(str, Enum):
    """Installation method for Deepnote Toolkit.

    BUNDLE: Traditional Docker-based installation with /deepnote-configs marker.
    PIP: Package installed via pip in site-packages or dist-packages.
    UNKNOWN: Installation method cannot be determined.
    """

    BUNDLE = "bundle"
    PIP = "pip"
    UNKNOWN = "unknown"


def get_installation_method() -> InstallMethod:
    """Detect installation method using a bundle marker (/deepnote-configs)
    and whether this module lives under site/dist-packages.
    Returns BUNDLE/PIP/UNKNOWN.
    """
    has_bundle_marker = Path("/deepnote-configs").exists()

    # Check if module is in site-packages or dist-packages (exact directory match)
    module_path = Path(str(files(__package__)))
    file_parts = module_path.parts
    in_site_packages = any(
        part in {"site-packages", "dist-packages"} for part in file_parts
    )

    if has_bundle_marker:
        return InstallMethod.BUNDLE
    if in_site_packages:
        return InstallMethod.PIP
    return InstallMethod.UNKNOWN
