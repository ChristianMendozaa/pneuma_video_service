from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import router as video_router

app = FastAPI(
    title="Video Generation Service",
    description="Service for generating videos using Vertex AI Veo-3.1 API dynamically.",
    version="1.0.0"
)

# Set up CORS for integration with other services if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(video_router, prefix="/api/v1/video", tags=["video"])

@app.get("/health")
def health_check():
    return {"status": "ok"}
