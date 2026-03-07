import json
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    GOOGLE_CREDENTIALS_JSON: str
    GCS_BUCKET_NAME: str
    GOOGLE_CLOUD_REGION: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def credentials_dict(self) -> dict:
        try:
            return json.loads(self.GOOGLE_CREDENTIALS_JSON)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse GOOGLE_CREDENTIALS_JSON as JSON.")
            raise e

    @property
    def project_id(self) -> str:
        return self.credentials_dict.get("project_id", "")

settings = Settings()
