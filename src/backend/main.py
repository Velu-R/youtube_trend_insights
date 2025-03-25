from fastapi import FastAPI
from utils import logger

logger = logger.get_logger()

app = FastAPI()

@app.get('/')
def root():
    logger.info("Started API")
    return {"message": "Welcome YouTrend Insights API"}