from fastapi import FastAPI, HTTPException
from urllib3 import request

from validators import DataValidator
from pydantic import BaseModel

app = FastAPI()

#Define a Pydantic model for the request body
class ValidateRequest(BaseModel):
    """
       Pydantic model for the data validation request.
    """
    payload_format: str
    payload_schema: str
    payload_content_base64: str

@app.post("/validate-data/")
async def validate_base64_data(request: ValidateRequest):
    """ API endpoints for validating data.
        A request model with payload_format, payload_schema and payload_content_base64
        Return response with validation results.
    """

    validator = DataValidator(request.payload_format, request.payload_schema, request.payload_content_base64)
    result = validator.validate()
    return result