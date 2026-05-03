# tests/conftest.py
import pytest
from dotenv import load_dotenv

# Load .env once for the whole suite so tests that probe os.environ work.
load_dotenv()


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests that hit real Aura/MinIO/OpenAI",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_marker = pytest.mark.skip(reason="needs --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
