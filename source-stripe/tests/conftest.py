from pathlib import Path
import pytest
import os


@pytest.fixture(scope="session")
def flow_path() -> str:
    current_path = Path(os.path.abspath(__name__))
    flow_file = current_path.parent.joinpath("test.flow.yaml")
    return str(flow_file)
