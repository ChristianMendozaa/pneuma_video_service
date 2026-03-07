from google.oauth2 import service_account
from app.core.config import settings

def get_gcp_credentials() -> service_account.Credentials:
    """
    Returns the parsed Service Account Credentials from the .env JSON.
    """
    creds_dict = settings.credentials_dict
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    return service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
