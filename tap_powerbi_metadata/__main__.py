"""Power BI metadata entry point."""

from __future__ import annotations

from tap_powerbi_metadata.tap import TapPowerBIMetadata

TapPowerBIMetadata.cli()
