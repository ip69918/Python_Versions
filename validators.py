from fastapi import HTTPException
import base64
import os
import json
from io import StringIO
import pandas as pd
import logging
import traceback



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


""" payload_format : str
         The format of the payload (either 'csv' or 'json').
     payload_schema : str
         JSON schema as a string that defines the data validation rules.
     payload_content_base64 : str
         Base64 encoded content of the file to be validated.
"""

class DataValidator:


    def __init__(self, payload_format: str, payload_schema: str, payload_content_base64: str):

        self.payload_format = payload_format
        self.payload_schema = payload_schema
        self.payload_content_base64 = payload_content_base64
        self.is_valid_schema = self.__validate_schema()

    def __validate_schema(self):

        # Strip the payload_schema to check for empty or invalid JSON
        payload_schema_stripped = self.payload_schema.strip()
        try:
            schema = json.loads(payload_schema_stripped)
            return bool(schema)      # Return True if schema is valid and not empty
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in payload schema.")
            full_traceback = traceback.format_exc()
            raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}\nTraceback: {full_traceback}")
            #return False

    def validate(self):

        if not self.is_valid_schema:
            raise HTTPException(status_code=400, detail="Invalid schema.")

        if self.payload_format == "csv":
            return self.validate_csv()
        #elif self.payload_format == "json":
            #return self.validate_json()
            #pass
        else:
            logger.error("Unsupported payload_format: %s", self.payload_format)
            raise HTTPException(status_code=400, detail="Unsupported format. Only 'csv' or 'json' is allowed.")

    #Validates CSV data against the schema
    def validate_csv(self):


        # Decode the Base64-encoded content
        try:
            decoded_content=base64.b64decode(self.payload_content_base64)
        except Exception as e:
            logger.error("Invalid Base64 content: %s", str(e))
            raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")

        # Read the CSV content from the decoded Base64 string
        try:
            df=pd.read_csv(StringIO(decoded_content.decode()))
        except Exception as e:
            logger.error("Failed to parse CSV content: %s", str(e))
            raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")

        # Parse the JSON schema
        try:
            schema = json.loads(self.payload_schema)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in payload schema.")
            raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")


        #Validate the data against the schema
        errors = self.validate_data(df,schema)
        if errors:
            return {"status" : "error", "errors": errors}
        else:
            return {"status" : "success", "message" : "Data is valid!" }


    # Function to validate the data based on the schema
    """Validates DataFrame columns against the schema.
    DataFrame containing data to validate.
    JSON schema for expected column types."""

    def validate_data(self, df, schema):

        errors =[]
        for column , expected_type in schema.items():
            if column not in df.columns:
                errors.append(f"Missing columns: {column}")
            else:
                actual_type= str(df[column].dtype)
                if actual_type != expected_type:
                    errors.append(f"Incorrect type for {column}. Expected {expected_type}, but got {actual_type}")
        return errors
