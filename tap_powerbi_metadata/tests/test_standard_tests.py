"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from dateutil.relativedelta import relativedelta
from singer_sdk.helpers._util import read_json_file

from singer_sdk.testing import get_tap_test_class, SuiteConfig

from tap_powerbi_metadata.tap import TapPowerBIMetadata

SAMPLE_CONFIG = read_json_file('.secrets/config.json')
SAMPLE_CONFIG['start_date'] = (datetime.datetime.now(datetime.timezone.utc) - relativedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%SZ')

TEST_SUITE_CONFIG = SuiteConfig(
)

TestTapPowerBiMetadata = get_tap_test_class(
    tap_class=TapPowerBIMetadata,
    config=SAMPLE_CONFIG,
    suite_config=TEST_SUITE_CONFIG
)
