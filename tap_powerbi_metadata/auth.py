"""Power BI authentication."""

from __future__ import annotations

import sys

from singer_sdk.authenticators import OAuthAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class OAuthActiveDirectoryAuthenticator(OAuthAuthenticator):
    """Authenticate to Power BI with an Azure AD client-credentials token."""

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Client-credentials token request body."""
        return {
            "grant_type": "client_credentials",
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
