"""PowerBIMetadata tap class."""

from __future__ import annotations

import sys

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)

from tap_powerbi_metadata.streams import (
    ActivityEventsStream,
    DataSourcesStream,
    WorkspaceInfoStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

PLUGIN_NAME = "tap-powerbi-metadata"

STREAM_TYPES = [
    ActivityEventsStream,
    WorkspaceInfoStream,
    DataSourcesStream,
]


class TapPowerBIMetadata(Tap):
    """PowerBIMetadata tap class."""

    name = "tap-powerbi-metadata"
    config_jsonschema = PropertiesList(
        Property("tenant_id", StringType, required=True),
        Property("client_id", StringType, required=True),
        Property("client_secret", StringType, required=True),
        Property("start_date", DateTimeType),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TapPowerBIMetadata.cli
