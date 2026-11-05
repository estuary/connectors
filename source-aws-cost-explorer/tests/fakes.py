from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from source_aws_cost_explorer import api
from source_aws_cost_explorer.models import AWSAccessKey

CREDENTIALS = AWSAccessKey(access_key_id="test-key", secret_access_key="test-secret")


@dataclass
class FakeCostExplorer:
    """Records what the connector asks of boto3 and replays canned responses."""

    responses: list[Any]
    client_options: dict[str, Any] = field(default_factory=dict)
    calls: list[dict[str, Any]] = field(default_factory=list)
    closed: bool = False


def install_cost_explorer(monkeypatch, responses: list[Any]) -> FakeCostExplorer:
    fake = FakeCostExplorer(responses)

    class Client:
        def get_cost_and_usage(self, **kwargs):
            fake.calls.append(deepcopy(kwargs))
            item = fake.responses[len(fake.calls) - 1]
            if isinstance(item, Exception):
                raise item
            return item

        def close(self):
            fake.closed = True

    class Session:
        def client(self, service, **kwargs):
            assert service == "ce"
            fake.client_options = kwargs
            return Client()

    monkeypatch.setattr(api, "Session", Session)
    return fake
