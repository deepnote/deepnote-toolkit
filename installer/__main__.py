"""Main script for install the Deepnote environment."""

import argparse
import logging
import os
import shlex
import sys
import sysconfig
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

# Use package-qualified imports so this module can be imported and executed consistently.
from .module.constants import BASH_PROMPT_SCRIPT, GIT_SSH_COMMAND
from .module.downloader import load_toolkit_bundle
from .module.helper import (
    append_to_file,
    generate_kernel_config,
    get_kernel_site_package_path,
    get_server_site_package_path,
    parse_bundle_arguments,
    wait_for_mount,
)
from .module.kernels import setup_non_python_kernels
from .module.symlinks import (
    create_home_work_symlink,
    create_notebook_to_jupyter_server_symlink,
    create_python_symlink,
    create_work_symlink,
)
from .module.virtual_environment import VirtualEnvironment

if TYPE_CHECKING:
    from deepnote_core.config import DeepnoteConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[setup.py] [%(asctime)s]: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger()


def setup_config_dir(
    root_path: str, target_path="/deepnote-configs", cfg=None
) -> Tuple[str, Dict[str, str]]:
    """
    Setup the configuration files for the Deepnote environment.

    Uses the unified resource preparation logic for consistency between
    pip and bundle installations.
    """
    logger.info("Setting up configuration files")
    # Use the unified resource preparation from deepnote_core
    from deepnote_core.config import prepare_runtime_resources
    from deepnote_core.config.resources import get_resources_source_path

    # Get the source path from the bundle
    source_path = get_resources_source_path(bundle_root=Path(root_path))

    # Use unified resource preparation with config persistence
    prepared = prepare_runtime_resources(
        cfg=cfg,
        source_path=source_path,
        target_dir=Path(target_path),
        apply_env=False,  # Don't apply env yet, will be done in set_jupyter_ipython_configs
        persist_config=True,  # Persist config during resource setup
    )
    return str(prepared.resources.path), prepared.resources.env


def configure_git_ssh():
    """
    Configure the Git SSH command if it is not already set.
    """
    if "GIT_SSH_COMMAND" not in os.environ:
        os.environ["GIT_SSH_COMMAND"] = GIT_SSH_COMMAND
    else:
        logger.warning("GIT_SSH_COMMAND already set")


def configure_github_https(
    config_dir_path: str, root_dir: Optional[Union[str, Path]] = None
):
    """
    Configure GitHub to use HTTPS for authentication.
    If root_dir is specified, the gitconfig will be created relative to that directory.
    """
    logger.info("Creating gitconfig file")
    github_credential_helper_path = os.path.join(
        config_dir_path, "scripts", "github_credential_helper.py"
    )
    github_config = f"""
    [credential "https://github.com"]
    useHttpPath = true
    helper = {github_credential_helper_path}
    """

    if root_dir:
        # Create etc directory under root_dir
        etc_dir = Path(root_dir) / "etc"
        etc_dir.mkdir(parents=True, exist_ok=True)
        gitconfig_path = str(etc_dir / "gitconfig")
    else:
        gitconfig_path = "/etc/gitconfig"

    append_to_file(gitconfig_path, textwrap.dedent(github_config).strip())


def set_jupyter_ipython_configs(
    venv: VirtualEnvironment,
    toolkit_bundle_path: str,
    config_directory_path: str,
    resource_env: Optional[Dict[str, str]] = None,
):
    """
    Set the Jupyter and IPython configuration paths.

    Uses resource environment variables from unified resource setup
    and layers bundle-specific paths on top.
    """
    logger.info("Setup config paths for jupyter and ipython")

    # Start with base resource environment variables
    if resource_env:
        for key, value in resource_env.items():
            os.environ[key] = value
    else:
        # Fallback to manual setup if no resource_env provided
        config_path = Path(config_directory_path)
        ipython_config_path = config_path / "ipython"
        jupyter_config_path = config_path / "jupyter"
        os.environ["IPYTHONDIR"] = str(ipython_config_path)
        os.environ["JUPYTER_CONFIG_DIR"] = str(jupyter_config_path)
        os.environ["JUPYTER_PREFER_ENV_PATH"] = "0"

    # Bundle-specific additions
    jupyter_path_server_bundle = os.path.join(
        toolkit_bundle_path, "server-libs", "share", "jupyter"
    )
    jupyter_bin_path = os.path.join(
        toolkit_bundle_path,
        "server-libs",
        "bin",
    )

    jupyter_path_deepnote_core = os.path.join(config_directory_path, "jupyter")
    kernel_config_directory = os.path.join(
        jupyter_path_deepnote_core, "kernels", "python3-venv"
    )
    os.makedirs(kernel_config_directory, exist_ok=True)

    with open(
        os.path.join(kernel_config_directory, "kernel.json"), "w", encoding="utf-8"
    ) as file:
        jupyter_config_path = os.environ.get(
            "JUPYTER_CONFIG_DIR", os.path.join(config_directory_path, "jupyter")
        )
        file.write(generate_kernel_config(f"{jupyter_config_path}/kernel-startup.sh"))

    os.environ["DEEPNOTE_VENV_SITE_PACKAGES_PATH"] = venv.site_packages_path
    os.environ["PATH"] = f"{os.environ['PATH']}:{jupyter_bin_path}"

    # Layer bundle server path into JUPYTER_PATH
    # The resource_env should have already set the base JUPYTER_PATH with deepnote_core path
    if "JUPYTER_PATH" in os.environ:
        os.environ["JUPYTER_PATH"] = (
            f"{os.environ['JUPYTER_PATH']}:{jupyter_path_server_bundle}"
        )
    else:
        # Fallback if resource_env wasn't provided
        os.environ["JUPYTER_PATH"] = (
            f"{jupyter_path_deepnote_core}:{jupyter_path_server_bundle}"
        )

    venv.append_env_variable("PATH", "$PATH:" + os.environ["PATH"])
    venv.append_env_variable("JUPYTER_PATH", os.environ["JUPYTER_PATH"])
    venv.append_env_variable("IPYTHONDIR", os.environ["IPYTHONDIR"])
    venv.append_env_variable("JUPYTER_CONFIG_DIR", os.environ["JUPYTER_CONFIG_DIR"])
    venv.append_env_variable("JUPYTER_PREFER_ENV_PATH", "0")


def configure_profile(venv: VirtualEnvironment):
    """
    Set the prompt script for the virtual
    """
    logger.info("Configuring prompt")
    profile_content = BASH_PROMPT_SCRIPT + "\n" + f". {venv.activate_file_path}"
    append_to_file("~/.profile", profile_content)


# Legacy function removed - now using centralized registry action handlers


def bootstrap():
    """
    Server bootstrapping function. Parses arguments and sets up the Deepnote environment.
    """

    parser = argparse.ArgumentParser(
        description="Setup script for Deepnote environment."
    )

    # Phase 1: Parse arguments
    bundle_args = parse_bundle_arguments(parser)

    # Phase 2: Load the toolkit bundle
    if bundle_args.bundle_path is None:
        toolkit_bundle_path = load_toolkit_bundle(
            bundle_args,
        )
    else:
        toolkit_bundle_path = bundle_args.bundle_path

    # Phase 3: Inject server site packages to sys.path so that installer can use them
    sys.path.insert(0, get_server_site_package_path(toolkit_bundle_path))

    # Phase 4: Persist effective configuration to a file for runtime (toolkit) consumption
    cfg: Optional[DeepnoteConfig] = None
    application_args: Optional[Any] = None
    config_target_path = "/deepnote-configs"
    config_directory_path = "/deepnote-configs"
    resource_env: Optional[Dict[str, str]] = None
    try:
        # Build and persist a DeepnoteConfig instance
        from deepnote_core.config.loader import ConfigurationLoader

        from .module.config_adapter import parse_application_arguments

        application_args = parse_application_arguments(parser)
        cfg = ConfigurationLoader().load_with_args(application_args)

        # If root_dir is specified, adjust config path to be relative to it
        config_target_path = (
            str(Path(cfg.paths.root_dir) / "deepnote-configs")
            if cfg.paths.root_dir
            else "/deepnote-configs"
        )
        config_directory_path, resource_env = setup_config_dir(
            toolkit_bundle_path, config_target_path, cfg
        )
        # Note: config is already persisted by prepare_runtime_resources with persist_config=True
    except Exception as e:
        logger.warning("Failed to persist effective configuration file: %s", e)

    if cfg is None:
        raise ValueError("Failed to load configuration")

    # Phase 5: Create symlinks for python and work if not running in detached mode
    if not cfg.runtime.running_in_detached_mode:
        wait_for_mount(
            cfg.paths.work_mountpoint, timeout=60, interval=0.25, logger=logger
        )
        create_work_symlink(cfg.paths.work_mountpoint)
        create_home_work_symlink()
        create_python_symlink()

    # Phase 6: Create the virtual environment
    venv = VirtualEnvironment(
        cfg.paths.venv_path, without_pip=cfg.runtime.venv_without_pip
    )
    venv.create()

    # Phase 7: Setup import paths for the virtual environment
    # Phase 7.1: Import site packages
    system_site_packages_path = sysconfig.get_path("purelib")
    server_site_packages_path = get_server_site_package_path(toolkit_bundle_path)
    kernel_site_package_path = get_kernel_site_package_path(toolkit_bundle_path)

    venv.import_package_bundle(
        server_site_packages_path, condition_env="DEEPNOTE_INCLUDE_SERVER_PACKAGES"
    )
    venv.import_package_bundle(system_site_packages_path)
    venv.import_package_bundle(kernel_site_package_path)

    # Phase 7.2: Symlink notebook to jupyter_server for compatibility with
    # custom kernels (e.g. Stata)
    create_notebook_to_jupyter_server_symlink(
        system_site_packages_path, server_site_packages_path
    )

    # Phase 7.3: Set environmental variable to load server site packages in server process
    os.environ["DEEPNOTE_INCLUDE_SERVER_PACKAGES"] = "true"

    # Phase 8: Setup folder structures based on configuration
    if cfg.paths.home_dir:
        os.makedirs(cfg.paths.home_dir, exist_ok=True)
    if cfg.paths.log_dir:
        os.makedirs(cfg.paths.log_dir, exist_ok=True)

    # Phase 9 (temp): Temporary set until all projects use DEEPNOTE_PROJECT_ID and DEEPNOTE_JUPYTER_TOKEN
    if os.getenv("PROJECT_ID"):
        os.environ["DEEPNOTE_PROJECT_ID"] = os.environ["PROJECT_ID"]
    if os.getenv("JUPYTER_TOKEN"):
        os.environ["DEEPNOTE_JUPYTER_TOKEN"] = os.environ["JUPYTER_TOKEN"]

    # Phase 10: Set up the environment variables for Jupyter and IPython
    set_jupyter_ipython_configs(
        venv, toolkit_bundle_path, config_directory_path, resource_env
    )

    # Make effective config discoverable to all child processes (server and kernel)
    if os.environ.get("DEEPNOTE_CONFIG_FILE"):
        venv.append_env_variable(
            "DEEPNOTE_CONFIG_FILE", shlex.quote(os.environ["DEEPNOTE_CONFIG_FILE"])
        )

    # Phase 11: Create necessary configurations files in a pod
    configure_git_ssh()
    configure_github_https(config_directory_path, cfg.paths.root_dir)
    configure_profile(venv)

    if not cfg.server.python_kernel_only:
        setup_non_python_kernels(venv, config_directory_path)

    logger.info("Preparation of the environment was successful. Starting server...")

    # Phase 12: Build plan and start servers
    # Build runtime plan for Jupyter/LSP/terminals
    from deepnote_core.runtime.plan import build_server_plan

    actions = build_server_plan(cfg)

    logger.info(f"Built runtime plan with {len(actions)} actions")

    # Start with the actions from build_server_plan
    all_actions = list(actions)

    # Add Streamlit actions if configured (fetch from webapp)
    from deepnote_core.runtime.types import StreamlitSpec

    if cfg.server.start_streamlit_servers:
        from .module.streamlit import fetch_streamlit_apps

        try:
            streamlit_apps = fetch_streamlit_apps(logger)
            for app in streamlit_apps:
                entrypoint = app.get("entrypoint")
                port = app.get("port")

                if not entrypoint or not port:
                    logger.warning(
                        f"Skipping app due to missing 'entrypoint' or 'port': {app}"
                    )
                    continue

                # Create StreamlitSpec action for each app
                # Note: The entrypoint is relative to /work
                all_actions.append(
                    StreamlitSpec(
                        script=f"/work/{entrypoint}",
                        port=port,
                        args=[
                            "--server.headless",
                            "true",
                            "--server.address",
                            "0.0.0.0",
                            "--server.enableCORS",
                            "false",
                            "--server.enableXsrfProtection",
                            "false",
                            "--server.runOnSave",
                            "true",
                            "--server.fileWatcherType",
                            "poll",
                        ],
                    )
                )
        except Exception as e:
            logger.error(f"Failed to fetch Streamlit apps: {e}")

    # TODO: Move Prometheus metrics server to build_server_plan() in follow-up PR
    # Currently kept here for backward compatibility until config structure is updated
    from deepnote_core.runtime.types import ExtraServerSpec

    # Add prometheus metrics server
    prometheus_script = os.path.join(
        config_directory_path, "scripts", "prometheus_metrics.py"
    )
    all_actions.append(ExtraServerSpec(command=["python", prometheus_script]))

    # Execute all actions via the unified registry
    from .module.executor import run_actions_in_installer_env

    server_processes = run_actions_in_installer_env(venv, all_actions, logger)

    return server_processes


def start_servers(server_processes):
    """Wait for already-started server processes to finish."""
    # All servers are already started by the execution context
    # Just wait for them to finish
    for server_process in server_processes:
        server_process.wait()


def main() -> None:
    servers = bootstrap()
    start_servers(servers)


if __name__ == "__main__":
    main()
