import datetime
from google.cloud import storage
from app.core.config import settings
from app.core.security import get_gcp_credentials

def get_bucket():
    creds = get_gcp_credentials()
    # Initialize the client with credentials and project
    client = storage.Client(credentials=creds, project=settings.project_id)
    return client.bucket(settings.GCS_BUCKET_NAME)

def get_output_uri(job_id: str) -> str:
    """Gets the GCS output URI for Veo predictions."""
    return f"gs://{settings.GCS_BUCKET_NAME}/videos/{job_id}/"

def generate_signed_url(blob_name: str) -> str:
    """Generates a 7-day signed URL for the generated video."""
    bucket = get_bucket()
    blob = bucket.blob(blob_name)
    
    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="GET",
    )
    return url
