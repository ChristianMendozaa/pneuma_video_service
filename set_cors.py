import os
import sys

# Añadir el entorno actual al path para importar módulos de la app
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from google.cloud import storage
from app.core.config import settings
from app.core.security import get_gcp_credentials

def set_bucket_cors():
    creds = get_gcp_credentials()
    client = storage.Client(credentials=creds, project=settings.project_id)
    bucket = client.bucket(settings.GCS_BUCKET_NAME)
    
    bucket.cors = [
        {
            "origin": ["*"],
            "method": ["GET", "OPTIONS"],
            "responseHeader": ["Content-Type", "Authorization", "Content-Length", "User-Agent", "x-goog-resumable"],
            "maxAgeSeconds": 3600
        }
    ]
    bucket.patch()
    print(f"✅ Reglas CORS configuradas exitosamente para el bucket: {settings.GCS_BUCKET_NAME}")

if __name__ == "__main__":
    set_bucket_cors()
