"""Stream class for tap-powerbi-metadata."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib import parse

import requests
from singer_sdk import Stream
from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_powerbi_metadata.client import TapPowerBIMetadataStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

API_DATE_FORMAT = "'%Y-%m-%dT%H:%M:%SZ'"
WORKSPACE_CHILD_OBJECT = ObjectType(additional_properties=True)


class ActivityEventsStream(TapPowerBIMetadataStream):
    """Power BI activity events for one UTC day at a time."""

    name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]  # ruff: ignore[mutable-class-default]
    replication_key = "CreationTime"
    schema: ClassVar[dict[str, Any]] = PropertiesList(
        # Keys
        Property("Id", StringType, required=True),
        Property("CreationTime", DateTimeType, required=True),
        # Properties
        Property("AccessRequestMessage", StringType),
        Property("AccessRequestType", StringType),
        Property("Activity", StringType),
        Property("ActivityId", StringType),
        Property(
            "AggregatedWorkspaceInformation",
            ObjectType(
                Property("WorkspaceCount", IntegerType),
                Property("WorkspacesByCapacitySku", StringType),
                Property("WorkspacesByType", StringType),
            ),
        ),
        Property("AppName", StringType),
        Property("AppId", StringType),
        Property("AppReportId", StringType),
        Property("ArtifactAccessRequestInfo", ObjectType(additional_properties=True)),
        Property("ArtifactId", StringType),
        Property("ArtifactKind", StringType),
        Property("ArtifactName", StringType),
        Property("ArtifactObjectId", StringType),
        Property(
            "AuditedArtifactInformation",
            ObjectType(
                Property("AnnotatedItemType", StringType),
                Property("ArtifactObjectId", StringType),
                Property("Id", StringType),
                Property("Name", StringType),
            ),
        ),
        Property("BillingType", IntegerType),
        Property("CapacityId", StringType),
        Property("CapacityName", StringType),
        Property("CapacityState", StringType),
        Property("CapacityUsers", StringType),
        Property("ClientIP", StringType),
        Property("ConsumptionMethod", StringType),
        Property("CopiedReportId", StringType),
        Property("CopiedReportName", StringType),
        Property("CredentialSetupMode", StringType),
        Property("CustomVisualAccessTokenResourceId", StringType),
        Property("CustomVisualAccessTokenSiteUri", StringType),
        Property("DashboardId", StringType),
        Property("DashboardName", StringType),
        Property("DataConnectivityMode", StringType),
        Property(
            "DataflowAccessTokenRequestParameters",
            ObjectType(
                Property("entityName", StringType),
                Property("partitionUri", StringType),
                Property("permissions", IntegerType),
                Property("tokenLifetimeInMinutes", IntegerType),
            ),
        ),
        Property("DataflowAllowNativeQueries", BooleanType),
        Property("DataflowId", StringType),
        Property("DataflowName", StringType),
        Property("DataflowRefreshScheduleType", StringType),
        Property("DataflowType", StringType),
        Property("DatasetCertificationStage", StringType),
        Property("DatasetId", StringType),
        Property("DatasetName", StringType),
        Property(
            "Datasets",
            ArrayType(
                ObjectType(
                    Property("DatasetId", StringType),
                    Property("DatasetName", StringType),
                )
            ),
        ),
        Property("DatasourceId", StringType),
        Property("DatasourceObjectIds", ArrayType(StringType)),
        Property("DatasourceInformations", ArrayType(ObjectType(additional_properties=True))),
        Property(
            "Datasources",
            ArrayType(
                ObjectType(
                    Property("ConnectionDetails", StringType),
                    Property("DatasourceType", StringType),
                )
            ),
        ),
        Property("DatasourceType", StringType),
        Property(
            "DeploymentPipelineAccesses",
            ArrayType(
                ObjectType(
                    Property("RolePermissions", StringType),
                    Property("UserObjectId", StringType),
                )
            ),
        ),
        Property("DeploymentPipelineDisplayName", StringType),
        Property("DeploymentPipelineId", IntegerType),
        Property("DeploymentPipelineObjectId", StringType),
        Property("DeploymentPipelineStageOrder", IntegerType),
        Property("DistributionMethod", StringType),
        Property("EndPoint", StringType),
        Property("Experience", StringType),
        Property("ExportedArtifactDownloadInfo", ObjectType(additional_properties=True)),
        Property(
            "ExportedArtifactInfo",
            ObjectType(
                Property("ArtifactId", IntegerType),
                Property("ArtifactType", StringType),
                Property("ExportType", StringType),
            ),
        ),
        Property("ExportEventActivityTypeParameter", StringType),
        Property("ExportEventEndDateTimeParameter", DateTimeType),
        Property("ExportEventStartDateTimeParameter", DateTimeType),
        Property("ExternalSubscribeeInformation", StringType),
        Property(
            "ExternalSubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                )
            ),
        ),
        Property(
            "FolderAccessRequests",
            ArrayType(
                ObjectType(
                    Property("GroupId", IntegerType),
                    Property("GroupObjectId", StringType),
                    Property("RolePermissions", StringType),
                    Property("UserId", IntegerType),
                    Property("UserObjectId", StringType),
                )
            ),
        ),
        Property("FolderDisplayName", StringType),
        Property("FolderObjectId", StringType),
        Property("GatewayClusterId", StringType),
        Property("GatewayClusterDatasources", ArrayType(ObjectType(additional_properties=True))),
        Property("GatewayClustersObjectIds", ArrayType(StringType)),
        Property(
            "GatewayClusters",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("memberGatewaysIds", ArrayType(StringType)),
                    Property(
                        "permissions",
                        ArrayType(
                            ObjectType(
                                Property("allowedDataSources", ArrayType(StringType)),
                                Property("id", StringType),
                                Property("principalType", StringType),
                                Property("role", StringType),
                            )
                        ),
                    ),
                    Property("type", StringType),
                )
            ),
        ),
        Property("GatewayId", StringType),
        Property("GatewayMemberId", StringType),
        Property("GatewayType", StringType),
        Property(
            "GenerateScreenshotInformation",
            ObjectType(
                Property("ExportFormat", StringType),
                Property("ExportType", IntegerType),
                Property("ExportUrl", StringType),
                Property("ScreenshotEngineType", IntegerType),
            ),
        ),
        Property("HasFullReportAttachment", BooleanType),
        Property("ImportDisplayName", StringType),
        Property("ImportId", StringType),
        Property("ImportSource", StringType),
        Property("ImportType", StringType),
        Property(
            "InstallTeamsAnalyticsInformation",
            ObjectType(
                Property("ModelId", StringType),
                Property("TenantId", StringType),
                Property("UserId", StringType),
            ),
        ),
        Property("IsSuccess", BooleanType),
        Property("IsTemplateAppFromMarketplace", BooleanType),
        Property("IsTenantAdminApi", BooleanType),
        Property("IsUpdateAppActivity", BooleanType),
        Property("ItemId", StringType),
        Property("ItemName", StringType),
        Property("LastRefreshTime", StringType),
        Property(
            "MembershipInformation",
            ArrayType(
                ObjectType(
                    Property("MemberEmail", StringType),
                )
            ),
        ),
        Property("MentionedUsersInformation", StringType),
        Property("ModelId", StringType),
        Property("ModelsSnapshots", ArrayType(IntegerType)),
        Property("Monikers", ArrayType(StringType)),
        Property("ObjectDisplayName", StringType),
        Property("ObjectId", StringType),
        Property("ObjectType", StringType),
        Property("Operation", StringType),
        Property("OrganizationId", StringType),
        Property(
            "OrgAppPermission",
            ObjectType(
                Property("permissions", StringType),
                Property("recipients", StringType),
            ),
        ),
        Property("OriginalOwner", StringType),
        Property("PackageId", IntegerType),
        Property(
            "PaginatedReportDataSources",
            ArrayType(
                ObjectType(
                    Property("connectionString", StringType),
                    Property("credentialRetrievalType", StringType),
                    Property("", StringType),
                    Property("name", StringType),
                    Property("provider", StringType),
                )
            ),
        ),
        Property(
            "PinReportToTabInformation",
            ObjectType(
                Property("ChannelId", StringType),
                Property("ChannelName", StringType),
                Property("DatasetId", StringType),
                Property("DatasetName", StringType),
                Property("ReportId", StringType),
                Property("ReportName", StringType),
                Property("TabName", StringType),
                Property("TeamId", StringType),
                Property("TeamName", StringType),
                Property("TeamsAppId", StringType),
                Property("UserId", StringType),
            ),
        ),
        Property("RecordType", IntegerType),
        Property("RefreshEnforcementPolicy", IntegerType),
        Property("RefreshType", StringType),
        Property("ReportCertificationStage", StringType),
        Property("ReportId", StringType),
        Property("ReportMobileLayoutAction", StringType),
        Property("ReportName", StringType),
        Property("ReportType", StringType),
        Property("RequestId", StringType),
        Property("ResultStatus", StringType),
        Property(
            "Schedules",
            ObjectType(
                Property("Days", ArrayType(StringType)),
                Property("RefreshFrequency", StringType),
                Property("Time", ArrayType(StringType)),
                Property("TimeZone", StringType),
            ),
        ),
        Property("ShareLinkId", StringType),
        Property("SharingAction", StringType),
        Property(
            "SharingInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                    Property("RecipientName", StringType),
                    Property("ObjectId", StringType),
                    Property("UserPrincipalName", StringType),
                    Property("ResharePermission", StringType),
                    Property("TenantObjectId", StringType),
                )
            ),
        ),
        Property("SharingScope", StringType),
        Property("SwitchState", StringType),
        Property("SubscriptionDetails", ObjectType(additional_properties=True)),
        Property(
            "SubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("ObjectId", StringType),
                    Property("RecipientEmail", StringType),
                    Property("RecipientName", StringType),
                )
            ),
        ),
        Property(
            "SubscriptionSchedule",
            ObjectType(
                Property("DaysOfTheMonth", StringType),
                Property("EndDate", DateTimeType),
                Property("StartDate", DateTimeType),
                Property("Time", ArrayType(StringType)),
                Property("TimeZone", StringType),
                Property("Type", StringType),
                Property("WeekDays", ArrayType(StringType)),
            ),
        ),
        Property("TableName", StringType),
        Property("TakingOverOwner", StringType),
        Property("TargetWorkspaceId", StringType),
        Property("TemplateAppFolderObjectId", StringType),
        Property("TemplateAppIsInstalledWithAutomation", BooleanType),
        Property("TemplateAppObjectId", StringType),
        Property("TemplateAppOwnerTenantObjectId", StringType),
        Property("TemplateAppVersion", StringType),
        Property("TemplatePackageName", StringType),
        Property("TileText", StringType),
        Property(
            "UpdateFeaturedTables",
            ArrayType(
                ObjectType(
                    Property("State", StringType),
                    Property("TableName", StringType),
                )
            ),
        ),
        Property("UserAgent", StringType),
        Property("UserId", StringType),
        Property(
            "UserInformation",
            ObjectType(
                Property("UsersAdded", ArrayType(StringType)),
                Property("UsersRemoved", ArrayType(StringType)),
            ),
        ),
        Property("UserKey", StringType),
        Property("UserType", IntegerType),
        Property("Workload", StringType),
        Property(
            "WorkspaceAccessList",
            ArrayType(
                ObjectType(
                    Property(
                        "UserAccessList",
                        ArrayType(
                            ObjectType(
                                Property("GroupUserAccessRight", StringType),
                                Property("Identifier", StringType),
                                Property("PrincipalType", StringType),
                                Property("UserEmailAddress", StringType),
                            )
                        ),
                    ),
                    Property("WorkspaceId", StringType),
                )
            ),
        ),
        Property("WorkspaceId", StringType),
        Property("WorkSpaceName", StringType),
        Property("WorkspacesSemicolonDelimitedList", StringType),
    ).to_dict()

    @override
    def get_url_params(
        self,
        context: Mapping[str, Any] | None,
        next_page_token: Any | None,
    ) -> dict[str, Any] | str:
        """Return a dictionary of values to be used in URL parameterization.

        API only supports a single UTC day, or continuationToken-based pagination.

        Raises:
            ValueError: If the start date or bookmark is older than the API allows.
        """
        params: dict[str, Any] = {}
        current_date = datetime.now(timezone.utc).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        token = cast("dict[str, Any]", next_page_token) if next_page_token else None
        if token:
            starting_datetime = token["urlStartDate"]
            continuation_token = token.get("continuationToken")
        else:
            starting_datetime = self.get_starting_timestamp(context)
            if starting_datetime:
                date_difference = current_date - starting_datetime
                if date_difference > timedelta(days=27):
                    self.logger.error("The start date or bookmark is more than 28 days ago.")
                    message = "The start date or bookmark is more than 28 days ago."
                    raise ValueError(message)
            else:
                starting_datetime = current_date - timedelta(days=27)
                self.logger.info(
                    "No start date or bookmark, sync last 28 days (%s).",
                    starting_datetime,
                )
            continuation_token = None

        if continuation_token:
            params["continuationToken"] = "'" + continuation_token + "'"
        else:
            params.update({"startDateTime": starting_datetime.strftime(API_DATE_FORMAT)})
            ending_datetime = (
                starting_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=1)
                + timedelta(microseconds=-1)
            )
            params.update({"endDateTime": ending_datetime.strftime(API_DATE_FORMAT)})
        self.logger.debug(params)
        return params

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        """Return a paginator that continues through empty UTC days."""
        return ActivityEventsPaginator(self)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: The activity-events HTTP response.

        Yields:
            Activity event records.
        """
        resp_json = response.json()
        yield from resp_json.get("activityEventEntities") or []


class ActivityEventsPaginator(BaseAPIPaginator):
    """Page activity events by UTC day, including days that contain no events."""

    def __init__(self, stream: ActivityEventsStream) -> None:
        """Create a paginator bound to one activity-events stream."""
        super().__init__(start_value=None)
        self.stream = stream

    @override
    def continue_if_empty(self, response: requests.Response) -> bool:
        """An empty UTC day is a normal remainder of the bookmarked date.

        Returns:
            Always true, so an empty page advances to the next day.
        """
        return True

    @override
    def get_next(self, response: requests.Response) -> dict | None:
        """Return the next continuation token or UTC day, or None when caught up."""
        resp_json = response.json()
        continuation_token = resp_json.get("continuationToken")
        previous_token = self.current_value
        next_page_token: dict = {}
        if not previous_token:
            req_url = response.request.url or ""
            req_params = parse.parse_qs(parse.urlparse(req_url).query)
            self.stream.logger.debug("Params: %s", req_params)
            latest_url_start_date_param = req_params["startDateTime"][0]
            next_page_token["urlStartDate"] = datetime.strptime(  # ruff: ignore[call-datetime-strptime-without-zone]
                latest_url_start_date_param,
                API_DATE_FORMAT,
            )
        else:
            next_page_token["urlStartDate"] = previous_token.get("urlStartDate")
        if continuation_token:
            next_page_token["continuationToken"] = requests.utils.unquote(continuation_token)
        else:
            next_page_token["continuationToken"] = None
            latest_url_start_date = next_page_token["urlStartDate"]
            next_url_start_date = latest_url_start_date.replace(
                hour=0,
                minute=0,
                second=0,
            ) + timedelta(days=1)
            self.stream.logger.info(
                "No next page token found, checking if %s is greater than now",
                next_url_start_date,
            )
            if next_url_start_date < datetime.now(timezone.utc).replace(tzinfo=None):
                self.stream.logger.info(
                    "%s is less than now, incrementing date by 1 and continuing",
                    next_url_start_date,
                )
                next_page_token["urlStartDate"] = next_url_start_date
                self.stream.logger.debug(next_page_token)
            else:
                self.stream.logger.info(
                    "No continuationToken, and nextUrlStartDate after today, calling it quits",
                )
                return None
        return next_page_token


class WorkspaceInfoStream(TapPowerBIMetadataStream):
    """Define custom stream."""

    name = "workspace_info"
    path = "/admin/workspaces/modified"
    http_method = "GET"
    primary_keys = ["id"]  # ruff: ignore[mutable-class-default]
    replication_key = None
    records_jsonpath = "$[*]"
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    schema: ClassVar[dict[str, Any]] = PropertiesList(
        Property("id", StringType),
        Property("name", StringType),
        Property("description", StringType),
        Property("type", StringType),
        Property("state", StringType),
        Property("dataRetrievalState", StringType),
        Property("isOnDedicatedCapacity", BooleanType),
        Property("capacityId", StringType),
        Property("defaultDatasetStorageFormat", StringType),
        Property("reports", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
        Property("dashboards", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
        Property("datasets", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
        Property("dataflows", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
        Property("datamarts", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
        Property("users", ArrayType(wrapped_type=WORKSPACE_CHILD_OBJECT)),
    ).to_dict()

    def process_workspace_batch(self, workspace_ids: list[str]) -> Iterable[dict]:
        """Request workspace info for a batch of workspaces.

        Args:
            workspace_ids: Workspace IDs to include in one scanner request.

        Yields:
            Workspace records from the completed scan.
        """
        workspace_info_req = (
            "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo"
            "?lineage=True"
            "&datasourceDetails=True"
            "&datasetSchema=False"
            "&datasetExpressions=False"
            "&getArtifactUsers=True"
        )

        workspace_info_req_body = {"workspaces": workspace_ids}

        info_query = requests.post(  # ruff: ignore[request-without-timeout]
            workspace_info_req,
            auth=self.authenticator,
            json=workspace_info_req_body,
        )

        info_query.raise_for_status()
        scan_id = info_query.json()["id"]
        status_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"
        finished = False
        sleep(3)

        while not finished:
            scan = requests.get(status_url, auth=self.authenticator)  # ruff: ignore[request-without-timeout]
            scan.raise_for_status()
            scan_status = scan.json()

            if scan_status["status"] == "Succeeded":
                finished = True
            else:
                sleep(10)

        final_result_r = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"

        result = requests.get(final_result_r, auth=self.authenticator)  # ruff: ignore[request-without-timeout]
        result.raise_for_status()

        rj = result.json()

        # The response includes every datasource referenced by the returned
        # workspaces. Attach them to the first record so the child stream can
        # receive them. They are absent from this stream's schema, so this
        # stream drops them after the child context is created.
        ds = rj.get("datasourceInstances", [])
        first_row = True

        for row in rj.get("workspaces", []):
            if first_row:
                row["datasources"] = ds
                first_row = False
            yield row

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The modified-workspaces HTTP response.

        Yields:
            Workspace records, including nested scanner results.
        """
        workspace_ids = [w["id"] for w in response.json()]
        workspace_chunks = [workspace_ids[i : i + 100] for i in range(0, len(workspace_ids), 100)]

        for chunk in workspace_chunks:
            yield from self.process_workspace_batch(chunk)

    @override
    def get_child_context(
        self,
        record: dict,
        context: Mapping[str, Any] | None,
    ) -> dict | None:
        """Return datasource records for the child stream.

        Args:
            record: A workspace record from the scanner.
            context: The parent stream context.

        Returns:
            Datasource instances for the first workspace, if the scan included any.
        """
        datasources = record.get("datasources")
        if datasources:
            return {"datasources": datasources}
        return None


class DataSourcesStream(Stream):
    """Define custom stream."""

    name = "datasources"
    primary_keys = ["datasourceId"]  # ruff: ignore[mutable-class-default]
    parent_stream_type = WorkspaceInfoStream
    ignore_parent_replication_key = False
    state_partitioning_keys = []  # ruff: ignore[mutable-class-default]
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    schema: ClassVar[dict[str, Any]] = PropertiesList(
        Property("datasourceId", StringType),
        Property("datasourceType", StringType),
        Property("gatewayId", StringType),
        Property("connectionDetails", ObjectType()),
    ).to_dict()

    @override
    def get_records(self, context: Mapping[str, Any] | None) -> Iterable[dict]:
        """Yield datasource records supplied by the parent workspace scan.

        Args:
            context: Child context containing datasource instances.

        Yields:
            Datasource records.
        """
        if context is None:
            return
        yield from context["datasources"]
