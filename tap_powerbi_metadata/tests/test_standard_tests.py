"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import logging
from pathlib import Path
from urllib import parse
from dateutil.relativedelta import relativedelta
from singer_sdk.helpers._typing import conform_record_data_types, TypeConformanceLevel
from singer_sdk.helpers._util import read_json_file

from singer_sdk.testing import get_tap_test_class, SuiteConfig

import requests

from tap_powerbi_metadata import streams
from tap_powerbi_metadata.streams import ActivityEventsStream, WorkspaceInfoStream
from tap_powerbi_metadata.tap import TapPowerBIMetadata

CONFIG_PATH = Path(".secrets/config.json")
SAMPLE_CONFIG = (
    read_json_file(CONFIG_PATH)
    if CONFIG_PATH.exists()
    else {
        "tenant_id": "tenant-id",
        "client_id": "client-id",
        "client_secret": "client-secret",
    }
)
SAMPLE_CONFIG["start_date"] = (
    datetime.datetime.now(datetime.timezone.utc) - relativedelta(days=5)
).strftime("%Y-%m-%dT%H:%M:%SZ")

if CONFIG_PATH.exists():
    TestTapPowerBiMetadata = get_tap_test_class(
        tap_class=TapPowerBIMetadata,
        config=SAMPLE_CONFIG,
        suite_config=SuiteConfig(),
    )


def test_workspace_child_objects_survive_recursive_conformance():
    """Workspace child fields must survive recursive target conformance."""
    workspace = {
        "id": "workspace-id",
        "name": "Workspace",
        "dataRetrievalState": "Succeeded",
        "reports": [{
            "id": "report-id",
            "name": "Report",
            "reportType": "PowerBIReport",
            "datasetId": "dataset-id",
            "users": [{"emailAddress": "reader@example.com"}],
        }],
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
        assert WorkspaceInfoStream.schema["properties"][child_name]["items"][
            "additionalProperties"
        ] is True


def test_process_workspace_batch_preserves_api_child_arrays(monkeypatch):
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
        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        streams.requests,
        "post",
        lambda *args, **kwargs: FakeResponse({"id": "scan-id"}),
    )
    responses = iter([
        FakeResponse({"status": "Succeeded"}),
        FakeResponse({
            "workspaces": [workspace],
            "datasourceInstances": [datasource],
        }),
    ])
    monkeypatch.setattr(streams.requests, "get", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(streams, "sleep", lambda *_: None)

    tap = TapPowerBIMetadata(config=SAMPLE_CONFIG)
    records = list(WorkspaceInfoStream(tap=tap).process_workspace_batch(["workspace-id"]))

    assert records == [{**workspace, "datasources": [datasource]}]
    assert records[0]["reports"][0]["id"] == "report-id"


class _NoAuth(requests.auth.AuthBase):
    def __call__(self, request):
        return request


def test_activity_events_continue_after_empty_day(monkeypatch):
    """An empty bookmarked day must still advance to the next UTC day."""
    event = {"Id": "event-id", "CreationTime": "2026-10-11T08:00:00Z"}
    bodies = [
        {"activityEventEntities": [], "continuationToken": None},
        {"activityEventEntities": [event], "continuationToken": None},
        {"activityEventEntities": None, "continuationToken": None},
    ]
    requested_urls = []

    class FakeResponse:
        def __init__(self, body, request):
            self.body = body
            self.request = request
            self.status_code = 200
            self.elapsed = datetime.timedelta(milliseconds=200)

        def json(self):
            return self.body

    def fake_request(prepared_request, context):
        requested_urls.append(prepared_request.url)
        return FakeResponse(bodies.pop(0), prepared_request)

    real_datetime = streams.datetime

    class FrozenDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime(2026, 10, 12, 15, 0, 0, tzinfo=tz)

    monkeypatch.setattr(streams, "datetime", FrozenDateTime)
    monkeypatch.setattr(
        ActivityEventsStream,
        "authenticator",
        property(lambda self: _NoAuth()),
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
    stream._write_starting_replication_value(None)
    monkeypatch.setattr(stream, "_request", fake_request)

    assert list(stream.request_records(None)) == [event]
    assert len(requested_urls) == 3

    def start_date(url):
        query = parse.urlparse(url).query
        return parse.parse_qs(query)["startDateTime"][0]

    assert start_date(requested_urls[0]) == "'2026-10-10T12:00:00Z'"
    assert start_date(requested_urls[1]) == "'2026-10-11T00:00:00Z'"
    assert start_date(requested_urls[2]) == "'2026-10-12T00:00:00Z'"
