"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

import datetime
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib import parse

import pytest
from requests.auth import AuthBase
from singer_sdk.helpers._typing import conform_record_data_types  # ruff: ignore[import-private-name]
from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_powerbi_metadata import streams
from tap_powerbi_metadata.streams import ActivityEventsStream, WorkspaceInfoStream
from tap_powerbi_metadata.tap import TapPowerBIMetadata

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from datetime import tzinfo

    import requests

CONFIG_PATH = Path(".secrets/config.json")
SAMPLE_CONFIG = (
    json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if CONFIG_PATH.exists()
    else {
        "tenant_id": "tenant-id",
        "client_id": "client-id",
        "client_secret": "client-secret",
    }
)
SAMPLE_CONFIG["start_date"] = (
    datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=5)
).strftime("%Y-%m-%dT%H:%M:%SZ")

if CONFIG_PATH.exists():
    TestTapPowerBiMetadata = get_tap_test_class(
        tap_class=TapPowerBIMetadata,
        config=SAMPLE_CONFIG,
        suite_config=SuiteConfig(),
    )
else:

    @pytest.mark.skip(reason="Power BI credentials are not configured")
    def test_tap_standard_suite() -> None:
        """The live SDK suite requires .secrets/config.json."""


def test_workspace_child_objects_survive_recursive_conformance() -> None:
    """Workspace child fields must survive recursive target conformance."""
    workspace = {
        "id": "workspace-id",
        "name": "Workspace",
        "dataRetrievalState": "Succeeded",
        "reports": [
            {
                "id": "report-id",
                "name": "Report",
                "reportType": "PowerBIReport",
                "datasetId": "dataset-id",
                "users": [{"emailAddress": "reader@example.com"}],
            }
        ],
        "dashboards": [{"id": "dashboard-id", "displayName": "Dashboard"}],
        "datasets": [{"id": "dataset-id", "name": "Dataset"}],
        "dataflows": [{"objectId": "dataflow-id", "name": "Dataflow"}],
        "datamarts": [{"id": "datamart-id", "name": "Datamart"}],
        "users": [{"emailAddress": "admin@example.com", "principalType": "User"}],
    }

    conformed = conform_record_data_types(
        stream_name=WorkspaceInfoStream.name,
        record=workspace,
        schema=WorkspaceInfoStream.schema,
        level=TypeConformanceLevel.RECURSIVE,
        logger=logging.getLogger(__name__),
    )

    assert conformed == workspace
    for child_name in (
        "reports",
        "dashboards",
        "datasets",
        "dataflows",
        "datamarts",
        "users",
    ):
        assert (
            WorkspaceInfoStream.schema["properties"][child_name]["items"]["additionalProperties"]
            is True
        )


def test_process_workspace_batch_preserves_api_child_arrays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tap must yield scanner child arrays without manufacturing empty objects."""
    workspace = {
        "id": "workspace-id",
        "reports": [{"id": "report-id", "name": "Report"}],
        "datasets": [{"id": "dataset-id", "name": "Dataset"}],
        "dashboards": [{"id": "dashboard-id", "displayName": "Dashboard"}],
        "dataflows": [{"objectId": "dataflow-id", "name": "Dataflow"}],
        "users": [{"emailAddress": "admin@example.com"}],
    }
    datasource = {"datasourceId": "datasource-id", "datasourceType": "Sql"}

    class FakeResponse:
        def __init__(self, body: dict) -> None:
            self.body = body

        def json(self) -> dict:
            return self.body

        def raise_for_status(self) -> None:
            assert self.body is not None

    monkeypatch.setattr(
        streams.requests,
        "post",
        lambda *_args, **_kwargs: FakeResponse({"id": "scan-id"}),
    )
    responses = iter([
        FakeResponse({"status": "Succeeded"}),
        FakeResponse({
            "workspaces": [workspace],
            "datasourceInstances": [datasource],
        }),
    ])
    monkeypatch.setattr(
        streams.requests,
        "get",
        lambda *_args, **_kwargs: next(responses),
    )
    monkeypatch.setattr(streams, "sleep", lambda *_args: None)

    tap = TapPowerBIMetadata(config=SAMPLE_CONFIG)
    records = list(WorkspaceInfoStream(tap=tap).process_workspace_batch(["workspace-id"]))

    assert records == [{**workspace, "datasources": [datasource]}]
    assert records[0]["reports"][0]["id"] == "report-id"


class _NoAuth(AuthBase):
    @override
    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        return r


def test_activity_events_continue_after_empty_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty bookmarked day must still advance to the next UTC day."""
    event = {"Id": "event-id", "CreationTime": "2026-10-11T08:00:00Z"}
    bodies: list[dict[str, Any]] = [
        {"activityEventEntities": [], "continuationToken": None},
        {"activityEventEntities": [event], "continuationToken": None},
        {"activityEventEntities": None, "continuationToken": None},
    ]
    requested_urls: list[str] = []

    class FakeResponse:
        def __init__(self, body: dict, request: requests.PreparedRequest) -> None:
            self.body = body
            self.request = request
            self.status_code = 200
            self.elapsed = datetime.timedelta(milliseconds=200)

        def json(self) -> dict:
            return self.body

    def fake_request(
        prepared_request: requests.PreparedRequest,
        context: dict | None,  # ruff: ignore[unused-function-argument]
    ) -> FakeResponse:
        requested_urls.append(prepared_request.url or "")
        return FakeResponse(bodies.pop(0), prepared_request)

    class FrozenDateTime(datetime.datetime):
        @classmethod
        @override
        def now(cls, tz: tzinfo | None = None) -> Self:
            return cls(2026, 10, 12, 15, 0, 0, tzinfo=tz)

    monkeypatch.setattr(streams, "datetime", FrozenDateTime)
    monkeypatch.setattr(
        ActivityEventsStream,
        "authenticator",
        property(lambda _self: _NoAuth()),
    )

    config = {**SAMPLE_CONFIG, "start_date": "2026-10-01T00:00:00Z"}
    tap = TapPowerBIMetadata(
        config=config,
        state={
            "bookmarks": {
                "ActivityEvents": {
                    "replication_key": "CreationTime",
                    "replication_key_value": "2026-10-10T12:00:00+00:00",
                }
            }
        },
    )
    stream = ActivityEventsStream(tap=tap)
    stream._write_starting_replication_value(None)  # ruff: ignore[private-member-access]
    monkeypatch.setattr(stream, "_request", fake_request)

    assert list(stream.request_records(None)) == [event]
    expected_requests = 3
    assert len(requested_urls) == expected_requests

    def start_date(url: str) -> str:
        query = parse.urlparse(url).query
        return parse.parse_qs(query)["startDateTime"][0]

    assert start_date(requested_urls[0]) == "'2026-10-10T12:00:00Z'"
    assert start_date(requested_urls[1]) == "'2026-10-11T00:00:00Z'"
    assert start_date(requested_urls[2]) == "'2026-10-12T00:00:00Z'"
