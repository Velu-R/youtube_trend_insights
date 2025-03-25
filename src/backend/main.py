from fastapi import FastAPI
from utils import logger
from schemas.api_response import APIResponse

logger = logger.get_logger()

app = FastAPI(title="YouTube Trend Insights API", description="API for YouTube Trend Analysis", version="1.0")

@app.get("/", response_model=APIResponse, status_code=200, tags=["Root"])
async def root():
    """Root endpoint for API health check and welcome message."""
    logger.info("API root accessed")
    return APIResponse(message="Welcome to YouTube Trend Insights API")