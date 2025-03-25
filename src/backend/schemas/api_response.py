from pydantic import BaseModel

# Define response model for structured response
class APIResponse(BaseModel):
    message: str