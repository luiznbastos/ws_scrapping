from typing import Optional
from enum import Enum
import boto3
from botocore.exceptions import ClientError
from pydantic import Field
from pydantic_settings import BaseSettings
import uuid
from datetime import datetime
from scrappers.utils.database import DatabaseClient
import os
from urllib.parse import quote_plus

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
    def database_client(self) -> DatabaseClient:
        if not hasattr(self, "_database_client"):
            ssm_path_prefix = f"/{self.project_name}/database"
            db_user = self._get_ssm_parameter(ssm_path_prefix + "/username")
            db_password = self._get_ssm_parameter(ssm_path_prefix + "/password")
            db_host = self._get_ssm_parameter(ssm_path_prefix + "/host")
            db_name = self._get_ssm_parameter(ssm_path_prefix + "/database")
            
            if db_user and db_password and db_host and db_name:
                encoded_password = quote_plus(db_password)
                db_url = f"redshift+redshift_connector://{db_user}:{encoded_password}@{db_host}:5439/{db_name}"
                print(f"Database connection: {db_user}@{db_host}:5439/{db_name}")
                self._database_client = DatabaseClient(db_url)
            else:
                missing = []
                if not db_user: missing.append("username")
                if not db_password: missing.append("password")
                if not db_host: missing.append("host")
                if not db_name: missing.append("database")
                raise ValueError(f"Missing database credentials from SSM: {', '.join(missing)}. Check SSM parameters.")
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
