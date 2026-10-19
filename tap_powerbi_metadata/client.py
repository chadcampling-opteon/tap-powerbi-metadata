"""Power BI REST client."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk.streams import RESTStream

from tap_powerbi_metadata.auth import OAuthActiveDirectoryAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.authenticators import APIAuthenticatorBase


class TapPowerBIMetadataStream(RESTStream):
    """Base stream for the Power BI REST API."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    @override
    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Azure AD authenticator for this stream."""
        return OAuthActiveDirectoryAuthenticator(
            auth_endpoint=(
                f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/v2.0/token"
            ),
            oauth_scopes="https://analysis.windows.net/powerbi/api/.default",
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
        )
