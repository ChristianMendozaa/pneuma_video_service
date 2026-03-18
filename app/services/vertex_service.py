import httpx
import base64
import google.auth.transport.requests
import re
from typing import Dict, Any, Optional

from app.core.config import settings
from app.core.security import get_gcp_credentials

async def get_access_token() -> str:
    creds = get_gcp_credentials()
    request = google.auth.transport.requests.Request()
    creds.refresh(request)
    return creds.token

def get_vertex_endpoint(path: str = "models/veo-3.1-fast-generate-001:predictLongRunning") -> str:
    region = settings.GOOGLE_CLOUD_REGION
    project_id = settings.project_id
    base_url = f"https://{region}-aiplatform.googleapis.com/v1"
    return f"{base_url}/projects/{project_id}/locations/{region}/publishers/google/{path}"

async def generate_video_async(
    prompt: str,
    duration_seconds: int,
    aspect_ratio: str,
    output_uri: str,
    image_bytes: Optional[bytes] = None,
    mime_type: str = "image/jpeg"
) -> str:
    token = await get_access_token()
    endpoint = get_vertex_endpoint()
    
    final_prompt = prompt
        
    instance_data = {
        "prompt": final_prompt
    }
    
    if image_bytes:
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")
        instance_data["image"] = {
            "bytesBase64Encoded": image_b64,
            "mimeType": mime_type
        }
        
    payload = {
        "instances": [instance_data],
        "parameters": {
            "generateAudio": True,
            "resolution": "1080p",
            "sampleCount": 1,
            "durationSeconds": duration_seconds,
            "aspectRatio": aspect_ratio,
            "personGeneration": "ALLOW_ADULT",
            "output_storage_uri": output_uri,
            "storageUri": output_uri
        }
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(endpoint, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Vertex AI API Error [{response.status_code}]: {response.text}")
            
        data = response.json()
        # Vertex predicting an LRO usually returns an 'name' which is the operation ID
        # or an 'operation' block containing 'name'
        if "name" in data:
            return data["name"] # This is the full operation name
        elif "operation" in data and "name" in data["operation"]:
            return data["operation"]["name"]
        else:
            raise Exception(f"Unexpected response format from Vertex AI: {data}")

async def extend_video_async(
    video_uri: str,
    prompt: str,
    output_uri: str,
    duration_seconds: int = 5,
    aspect_ratio: str = "16:9"
) -> str:
    token = await get_access_token()
    endpoint = get_vertex_endpoint()
    
    final_prompt = prompt
        
    payload = {
        "instances": [
            {
                "prompt": final_prompt,
                "video": {
                    "gcsUri": video_uri,
                    "mimeType": "video/mp4"
                }
            }
        ],
        "parameters": {
            "generateAudio": False,
            "resolution": "1080p",
            "sampleCount": 1,
            "durationSeconds": duration_seconds,
            "aspectRatio": aspect_ratio,
            "personGeneration": "ALLOW_ADULT",
            "output_storage_uri": output_uri,
            "storageUri": output_uri
        }
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(endpoint, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Vertex AI API Error [{response.status_code}]: {response.text}")
            
        data = response.json()
        if "name" in data:
            return data["name"]
        elif "operation" in data and "name" in data["operation"]:
            return data["operation"]["name"]
        else:
            raise Exception(f"Unexpected response format from Vertex AI: {data}")

async def get_operation_status(operation_name: str) -> Dict[str, Any]:
    # Veo predictLongRunning returns strings like:
    # projects/123/locations/us-central1/publishers/google/models/veo-3.1-fast-generate-001/operations/123-abc
    
    # We need to extract the model_endpoint to build the fetchPredictOperation URL
    match = re.search(r'(projects/[^/]+/locations/[^/]+/publishers/[^/]+/models/[^/]+)/operations/[^/]+', operation_name)
    
    if not match:
        raise ValueError(f"Invalid operation_name format for Veo 3.1: {operation_name}")
        
    model_endpoint = match.group(1)
        
    token = await get_access_token()
    region = settings.GOOGLE_CLOUD_REGION
    
    url = f"https://{region}-aiplatform.googleapis.com/v1/{model_endpoint}:fetchPredictOperation"
    
    payload = {
        "operationName": operation_name
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to get operation status [{response.status_code}]: {response.text}")
            
        return response.json()
