"""Pytest configuration."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest."""
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3\\.10 .* its end of life .*:FutureWarning",
        )
