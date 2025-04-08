"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Optional
from urllib import parse
import requests


from singer_sdk import Stream
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator, OAuthAuthenticator, OAuthJWTAuthenticator
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)
from singer_sdk.helpers._typing import TypeConformanceLevel

API_DATE_FORMAT = "'%Y-%m-%dT%H:%M:%SZ'"

class OAuthActiveDirectoryAuthenticator(OAuthAuthenticator):
    # https://pivotalbi.com/automate-your-power-bi-dataset-refresh-with-python

    @property
    def oauth_request_body(self) -> dict:
        return {
            'grant_type': 'client_credentials',
            'scope': 'https://analysis.windows.net/powerbi/api/.default',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
        }


class TapPowerBIMetadataStream(RESTStream):
    """PowerBIMetadata stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"


    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return OAuthActiveDirectoryAuthenticator(
            stream=self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/v2.0/token",
            oauth_scopes="https://analysis.windows.net/powerbi/api/.default",

        )




class ActivityEventsStream(TapPowerBIMetadataStream):
    name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]
    replication_key = "CreationTime"
    schema = PropertiesList(
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
            )
        ),
        Property("AppName", StringType),
        Property("AppId", StringType),
        Property("AppReportId", StringType),
        Property("ArtifactAccessRequestInfo", ObjectType(additional_properties=True)),
        Property("ArtifactId", StringType),
        Property("ArtifactKind", StringType),
        Property("ArtifactName", StringType),
        Property("ArtifactObjectId", StringType),
        Property("AuditedArtifactInformation",
            ObjectType(
                Property("AnnotatedItemType", StringType),
                Property("ArtifactObjectId", StringType),
                Property("Id", StringType),
                Property("Name", StringType),
            )
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
            )
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
            )
        ),
        Property("DatasourceId", StringType),
        Property("DatasourceObjectIds", ArrayType(StringType)),
        Property("DatasourceInformations", ArrayType(ObjectType(additional_properties=True))),
        Property("Datasources",  
            ArrayType(
                ObjectType(
                    Property("ConnectionDetails", StringType),
                    Property("DatasourceType", StringType),
                )
            )
        ),
        Property("DatasourceType", StringType),
        Property(
            "DeploymentPipelineAccesses",
            ArrayType(
                ObjectType(
                    Property("RolePermissions", StringType),
                    Property("UserObjectId", StringType),
                )
            )
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
            )
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
            )
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
            )
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
                        )
                    ),
                    Property("type", StringType),
                )
            )
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
            )
        ),
        Property("HasFullReportAttachment", BooleanType),
        Property("ImportDisplayName", StringType),
        Property("ImportId", StringType),
        Property("ImportSource", StringType),
        Property("ImportType", StringType),
        Property("InstallTeamsAnalyticsInformation",
            ObjectType(
                Property("ModelId", StringType),
                Property("TenantId", StringType),
                Property("UserId", StringType),
            )
        ),
        Property("IsSuccess", BooleanType),
        Property("IsTemplateAppFromMarketplace", BooleanType),
        Property("IsTenantAdminApi", BooleanType),
        Property("IsUpdateAppActivity", BooleanType),
        Property("ItemId", StringType),
        Property("ItemName", StringType),
        Property("LastRefreshTime", StringType),
        Property("MembershipInformation", 
            ArrayType(
                ObjectType(
                    Property("MemberEmail", StringType),
                )
            )
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
            )
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
            )
        ),
        Property("PinReportToTabInformation", 
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
            )
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
            )
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
            )
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
            )
        ),
        Property(
            "SubscriptionSchedule",
            ObjectType(
                Property("DaysOfTheMonth",StringType),
                Property("EndDate", DateTimeType),
                Property("StartDate", DateTimeType),
                Property(
                    "Time",
                    ArrayType(StringType)
                ),
                Property("TimeZone", StringType),
                Property("Type", StringType),
                Property(
                    "WeekDays",
                    ArrayType(StringType)
                ),
            )
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
            )
        ),
        Property("UserAgent", StringType),
        Property("UserId", StringType),
        Property(
            "UserInformation",
            ObjectType(
                Property(
                    "UsersAdded",
                    ArrayType(StringType)
                ),
                Property(
                    "UsersRemoved",
                    ArrayType(StringType)
                ),
            )
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
            )
        ),
        Property("WorkspaceId", StringType),
        Property("WorkSpaceName", StringType),
        Property("WorkspacesSemicolonDelimitedList", StringType),
    ).to_dict()
    
    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        
        API only supports a single UTC day, or continuationToken-based pagination.
        """
        params = {}
        current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        if next_page_token:
            starting_datetime = next_page_token["urlStartDate"]
            continuationToken = next_page_token.get("continuationToken")
        else:
            starting_datetime = self.get_starting_timestamp(partition)
            if starting_datetime :
                date_difference = current_date - starting_datetime
                if date_difference > timedelta(days=27):
                    self.logger.error("The start date or bookmark is more than 28 days ago.")
                    raise ValueError("The start date or bookmark is more than 28 days ago.")
            else:
                starting_datetime = current_date - timedelta(days=27)
                self.logger.info(f"No start date or bookmark, sync last 28 days ({starting_datetime}).")
            continuationToken = None

        if continuationToken:
            params["continuationToken"] = "'" + continuationToken + "'"
        else:
            params.update({"startDateTime": starting_datetime.strftime(API_DATE_FORMAT)})
            ending_datetime = starting_datetime.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1) + timedelta(microseconds=-1)
            params.update({"endDateTime": ending_datetime.strftime(API_DATE_FORMAT)})
        self.logger.debug(params)
        return params
    
    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        resp_json = response.json()
        continuationToken = resp_json.get("continuationToken")
        next_page_token = {}
        if not previous_token:
            # First time creating a pagination token so we need to record the initial start date.
            req_url = response.request.url
            req_params = parse.parse_qs(parse.urlparse(req_url).query)
            self.logger.debug("Params: {}".format(req_params))
            latest_url_start_date_param = req_params["startDateTime"][0]
            next_page_token["urlStartDate"] = datetime.strptime(latest_url_start_date_param, API_DATE_FORMAT)
        else: 
            next_page_token["urlStartDate"] = previous_token.get("urlStartDate")
        if continuationToken:
            next_page_token["continuationToken"] = requests.utils.unquote(continuationToken)
        else:
            next_page_token["continuationToken"] = None
            # Now check if we should repeat API call for next day
            latestUrlStartDate = next_page_token["urlStartDate"]
            nextUrlStartDate = latestUrlStartDate.replace(hour=0, minute=0, second=0) + timedelta(days=1)
            self.logger.info("No next page token found, checking if {} is greater than now".format(nextUrlStartDate))
            if nextUrlStartDate < datetime.utcnow():
                self.logger.info("{} is less than now, incrementing date by 1 and continuing".format(nextUrlStartDate))
                next_page_token["urlStartDate"] = nextUrlStartDate
                self.logger.debug(next_page_token)
            else:
                self.logger.info("No continuationToken, and nextUrlStartDate after today, calling it quits")
                return None
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json.get("activityEventEntities"):
            yield row
            
class WorkspaceInfoStream(TapPowerBIMetadataStream):
    """Define custom stream."""

    name = "workspace_info"
    path = "/admin/workspaces/modified"
    rest_method = "GET"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$[*]"
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY


    schema = PropertiesList(
        Property("id", StringType),
        Property("name", StringType),
        Property("description", StringType),
        Property("type", StringType),
        Property("state", StringType),
        Property("isOnDedicatedCapacity", BooleanType),
        Property("capacityId", StringType),
        Property("defaultDatasetStorageFormat", StringType),
        Property("reports", ArrayType(wrapped_type=ObjectType())),
        Property("dashboards", ArrayType(wrapped_type=ObjectType())),
        Property("datasets", ArrayType(wrapped_type=ObjectType())),
        Property("dataflows", ArrayType(wrapped_type=ObjectType())),
        Property("datamarts", ArrayType(wrapped_type=ObjectType())),
        Property("users", ArrayType(wrapped_type=ObjectType())),
    ).to_dict()


    def process_workspace_batch(self, workspace_ids):
        """
        Request workspace info for a batch of workspaces.
        """

        workspace_info_req = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo' \
                            '?lineage=True' \
                            '&datasourceDetails=True' \
                            '&datasetSchema=False' \
                            '&datasetExpressions=False' \
                            '&getArtifactUsers=True' 

        workspace_info_req_body = {
        "workspaces": workspace_ids
        }

        info_query = requests.post(
            workspace_info_req, 
            auth=self.authenticator,
            json=workspace_info_req_body
            )
        
        info_query.raise_for_status()
        scan_id = info_query.json()["id"]
        status_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"
        finished = False
        sleep(3)
        
        while not finished:
            
            scan = requests.get(status_url, auth=self.authenticator)
            scan.raise_for_status()
            scan_status = scan.json()

            if scan_status["status"] == "Succeeded":
                finished = True
            else:
                sleep(10)

        final_result_r = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"

        result = requests.get(final_result_r, auth=self.authenticator)
        result.raise_for_status()

        rj = result.json()

        # response includes all datasource referenced by returned workspaces. Stuff it into the record to shuffle to child context.
        # as it isn't in schema it will be dropped from this stream but will be processed by child stream.
        ds = rj["datasourceInstances"]
        first_row = True


        for row in rj["workspaces"]:
            if first_row:
                row["datasources"] = ds
                first_row = False
            yield row



    def parse_response(self, response: requests.Response):
        """Parse the response and return an iterator of result records.
        """

        workspace_ids = [w['id'] for w in response.json()]
        workspace_chunks = [workspace_ids[i:i + 100] for i in range(0, len(workspace_ids), 100)]

        for chunk in workspace_chunks:
            yield from self.process_workspace_batch(chunk)
            
    def get_child_context(self, record, context):
        datasources = record.get("datasources")
        if datasources:
            return {"datasources": datasources}
        return None

            
class DataSourcesStream(Stream):
    """Define custom stream."""

    name = "datasources"
    primary_keys = ["datasourceId"]
    parent_stream_type = WorkspaceInfoStream
    ignore_parent_replication_key = False
    state_partitioning_keys = []
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY
    
    schema = PropertiesList(
        Property("datasourceId", StringType),
        Property("datasourceType", StringType),
        Property("gatewayId", StringType),
        Property("connectionDetails", ObjectType()),
    ).to_dict()
    
    def get_records(self, context):
        for record in context["datasources"]:
            yield record

