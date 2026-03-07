import datetime
from google.cloud import firestore
from app.core.config import settings
from app.core.security import get_gcp_credentials

def get_firestore_client() -> firestore.Client:
    creds = get_gcp_credentials()
    return firestore.Client(credentials=creds, project=settings.project_id)

def create_video_job(video_id: str, operation_id: str, metadata: dict) -> None:
    db = get_firestore_client()
    doc_ref = db.collection("video_jobs").document(video_id)
    
    doc_ref.set({
        "video_id": video_id,
        "operation_id": operation_id,
        "status": "PROCESSING",
        "created_at": firestore.SERVER_TIMESTAMP,
        "metadata": metadata
    })

def get_video_job(video_id: str) -> dict:
    db = get_firestore_client()
    doc_ref = db.collection("video_jobs").document(video_id)
    doc = doc_ref.get()
    
    if doc.exists:
        return doc.to_dict()
    return None

def update_video_job(video_id: str, updates: dict) -> None:
    db = get_firestore_client()
    doc_ref = db.collection("video_jobs").document(video_id)
    
    # Adding updated_at timestamp natively
    if "updated_at" not in updates:
        updates["updated_at"] = firestore.SERVER_TIMESTAMP
        
    doc_ref.update(updates)

def list_video_jobs() -> list:
    db = get_firestore_client()
    docs = db.collection("video_jobs").order_by("created_at", direction=firestore.Query.DESCENDING).stream()
    
    jobs = []
    for doc in docs:
        job = doc.to_dict()
        # Convert DatetimeWithNanoseconds to string for JSON serialization
        if "created_at" in job and hasattr(job["created_at"], "isoformat"):
            job["created_at"] = job["created_at"].isoformat()
        if "updated_at" in job and hasattr(job["updated_at"], "isoformat"):
            job["updated_at"] = job["updated_at"].isoformat()
        jobs.append(job)
        
    return jobs
