from typing import Optional
from enum import Enum
import boto3
from botocore.exceptions import ClientError
from pydantic import Field
from pydantic_settings import BaseSettings
import uuid
from datetime import datetime
from scrappers.utils.duckdb_client import DuckDBClient
import os

class ScrappingType(str, Enum):
    DAILY = "DAILY"
    DATE_RANGE = "DATE_RANGE"
    FULL_RUN = "FULL_RUN"

class DriverType(str, Enum):
    REMOTE = "REMOTE"
    CHROMIUM = "CHROMIUM"


class AppSettings(BaseSettings):
    class Config:
        case_sensitive = False

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_ts: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Project settings for SSM
    project_name: str = Field(default="ws-analytics")
    stage: str = Field(default="stg")

    # Runner settings
    scrapping_type: ScrappingType = Field(default=ScrappingType.DAILY)
    driver_type: DriverType = Field(default=DriverType.CHROMIUM)
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)

    # WhoScored settings
    tournament_name: Optional[str] = Field(default=None)
    tournament_url: Optional[str] = Field(default=None)
    season: Optional[str] = Field(default=None)
    match: Optional[str] = Field(default=None)

    # Backfill: force re-scrape per step
    force_refresh_seasons: bool = Field(default=False)
    force_refresh_matches: bool = Field(default=False)
    force_refresh_events: bool = Field(default=False)


    def _get_ssm_parameter(self, name: str) -> Optional[str]:
        if not self._ssm_client:
            return None
        try:
            response = self._ssm_client.get_parameter(Name=name, WithDecryption=True)
            return response.get("Parameter", {}).get("Value")
        except (ClientError, self._ssm_client.exceptions.ParameterNotFound):
            print(f"Warning: Parameter {name} not found in SSM.")
            return None

    @property
    def _ssm_client(self):
        if not hasattr(self, "__ssm_client"):
            try:
                region = os.environ.get("AWS_REGION", "us-east-1")
                self.__ssm_client = boto3.client("ssm", region_name=region)
            except ClientError as e:
                print(f"Warning: Could not create Boto3 SSM client. Error: {e}")
                self.__ssm_client = None
        return self.__ssm_client
    
    @property
    def database_client(self) -> DuckDBClient:
        if not hasattr(self, "_database_client"):
            bucket = self.s3_bucket
            if not bucket:
                raise ValueError("Missing S3 bucket — check SSM parameter /ws-analytics/s3/analytics/name")
            region = os.environ.get("AWS_REGION", "us-east-1")
            session = boto3.Session(region_name=region)
            creds = session.get_credentials().get_frozen_credentials()
            self._database_client = DuckDBClient(
                bucket=bucket,
                run_id=self.run_id,
                aws_region=region,
                aws_access_key_id=creds.access_key,
                aws_secret_access_key=creds.secret_key,
                aws_session_token=creds.token,
            )
        return self._database_client


    @property
    def s3_bucket(self) -> Optional[str]:
        if not hasattr(self, "_s3_bucket"):
            ssm_path = f"/{self.project_name}/s3/analytics/name"
            self._s3_bucket = self._get_ssm_parameter(ssm_path)
        return self._s3_bucket

    @property
    def s3(self) -> boto3.client:
        if not hasattr(self, "_s3_client"):
            region = os.environ.get("AWS_REGION", "us-east-1")
            session = boto3.Session(region_name=region)
            self._s3_client = session.client("s3")
        return self._s3_client

    
    @property
    def network_driver(self):
        if not hasattr(self, "_network_driver"):
            from scrappers.driver.network_driver import ChromeNetworkDriver
            self._network_driver = ChromeNetworkDriver()
        return self._network_driver

settings = AppSettings()
