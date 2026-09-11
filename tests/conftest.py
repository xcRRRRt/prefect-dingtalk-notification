from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TypedDict
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from prefect.settings import PREFECT_API_URL as PREFECT_API_URL_SETTING
from prefect.settings import temporary_settings

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.test.yml"
PREFECT_API_URL = "http://127.0.0.1:14200/api"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run tests that call a real Prefect server and DingTalk webhook",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: requires Docker and real external credentials"
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--run-integration"):
        return

    skip = pytest.mark.skip(reason="use --run-integration to run external tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


def _read_dotenv() -> dict[str, str]:
    path = ROOT / ".env"
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _credential(name: str, dotenv: dict[str, str]) -> str:
    value = os.environ.get(name) or dotenv.get(name)
    if value:
        return value

    # Local interactive runs may enter credentials after Docker is ready. CI
    # has no TTY and must provide required values as secrets or in .env.
    if not sys.stdin.isatty():
        pytest.fail(
            f"missing {name}; set it in .env or the environment before running "
            "the integration tests"
        )
    prompt = f"{name}: "
    if name.endswith("SECRET"):
        import getpass

        value = getpass.getpass(prompt)
    else:
        value = input(prompt).strip()
    if not value:
        pytest.fail(f"{name} must not be empty")
    return value


def _optional_credential(name: str, dotenv: dict[str, str]) -> str | None:
    value = os.environ.get(name) or dotenv.get(name)
    return value or None


class DingTalkCredentials(TypedDict):
    access_token: str
    secret: str | None
    keyword: str


@pytest.fixture(scope="session")
def prefect_server(request: pytest.FixtureRequest) -> Iterator[str]:
    """Build and run an isolated Prefect server for integration tests."""
    if not request.config.getoption("--run-integration"):
        pytest.skip("use --run-integration to run external tests")

    project = f"prefect-dingtalk-notification-test-{os.getpid()}"
    compose = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        str(COMPOSE_FILE),
    ]
    settings_context = None
    saved_environment = {
        name: os.environ.get(name)
        for name in ("PREFECT_API_URL", "ALL_PROXY", "all_proxy")
    }
    try:
        # Pull the floating Prefect 3 base tag explicitly. The Compose service
        # itself is built locally and has no registry image to pull.
        subprocess.run(
            ["docker", "pull", "prefecthq/prefect:3-latest"],
            cwd=ROOT,
            check=True,
        )
        subprocess.run([*compose, "up", "--build", "--detach"], cwd=ROOT, check=True)

        deadline = time.monotonic() + 180
        health_url = f"{PREFECT_API_URL}/health"
        while time.monotonic() < deadline:
            try:
                with urlopen(health_url, timeout=3) as response:
                    if response.status == 200:
                        break
            except (OSError, URLError):
                time.sleep(2)
        else:
            logs = subprocess.run(
                [*compose, "logs", "--no-color", "prefect"],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            pytest.fail(
                f"Prefect test server did not become healthy:\n{logs.stdout[-4000:]}"
            )

        os.environ["PREFECT_API_URL"] = PREFECT_API_URL
        # httpx otherwise tries to initialize the optional SOCKS transport from
        # ALL_PROXY even for the local Prefect URL. Keep HTTP(S)_PROXY available
        # for the DingTalk request while bypassing the SOCKS-only setting.
        os.environ.pop("ALL_PROXY", None)
        os.environ.pop("all_proxy", None)
        # Prefect settings are materialized before pytest fixtures run. Set the
        # API URL in a settings context so every client and the event worker use
        # this isolated server instead of the user's active profile.
        settings_context = temporary_settings(
            updates={PREFECT_API_URL_SETTING: PREFECT_API_URL}
        )
        settings_context.__enter__()
        yield PREFECT_API_URL
    finally:
        if settings_context is not None:
            settings_context.__exit__(None, None, None)
        for name, value in saved_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        subprocess.run(
            [*compose, "down", "--volumes", "--remove-orphans"],
            cwd=ROOT,
            check=False,
        )


@pytest.fixture(scope="session")
def dingtalk_credentials(prefect_server: str) -> DingTalkCredentials:
    dotenv = _read_dotenv()
    return {
        "access_token": _credential("DINGTALK_ACCESS_TOKEN", dotenv),
        "secret": _optional_credential("DINGTALK_SECRET", dotenv),
        "keyword": os.environ.get("DINGTALK_KEYWORD")
        or dotenv.get("DINGTALK_KEYWORD", "Prefect"),
    }
