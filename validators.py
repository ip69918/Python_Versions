

from fastapi import HTTPException
import base64
import os
import json
from io import StringIO
import pandas as pd
import logging



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



class DataValidator:
    """
     A class used to validate data in CSV or JSON formats based on a schema.


     ----------
     payload_format : str
         The format of the payload (either 'csv' or 'json').
     payload_schema : str
         JSON schema as a string that defines the data validation rules.
     payload_content_base64 : str
         Base64 encoded content of the file to be validated.

     """

    def __init__(self, payload_format: str, payload_schema: str, payload_content_base64: str):

        """
                Initializes the DataValidator with payload format, schema, and content.

                Parameters:
                ----------
                payload_format : str
                    Format of the payload (csv/json).
                payload_schema : str
                    JSON schema for validation.
                payload_content_base64 : str
                    Base64 encoded data content.
        """
        self.payload_format = payload_format
        self.payload_schema = payload_schema
        self.payload_content_base64 = payload_content_base64
        self.is_valid_schema = self.__validate_schema()

    def __validate_schema(self):

        """
               Validates that the schema is a well-formed JSON object.

               Returns:
               -------
               bool
                   True if the schema is valid, raises HTTPException otherwise.
        """

        # Strip the payload_schema to check for empty or invalid JSON
        payload_schema_stripped = self.payload_schema.strip()
        try:
            schema = json.loads(payload_schema_stripped)
            return bool(schema)  # Return True if schema is valid and not empty
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in payload schema.")
            return False

    def validate(self):
        """
                Determines which validation method to use based on payload format.

                Returns:
                -------

                    Result of the validation.

                Raises:
                ------
                HTTPException
                    If payload format is unsupported or schema is invalid.
        """

        if not self.is_valid_schema:
            #logger.error("Invalid schema.")
            raise HTTPException(status_code=400, detail="Invalid schema.")

        if self.payload_format == "csv":
            return self.validate_csv()
        #elif self.payload_format == "json":
            #return self.validate_json()
            #pass
        else:
            #logger.error("Unsupported payload_format: %s", self.payload_format)
            raise HTTPException(status_code=400, detail="Unsupported format. Only 'csv' or 'json' is allowed.")

    def validate_csv(self):

        """
               Validates CSV data against the schema.

               Returns:
               -------
                   Validation result.

               Raises:
               ------
               HTTPException
                   For Base64 decoding errors or CSV parsing errors.
        """

        # Decode the Base64-encoded content
        try:
            decoded_content=base64.b64decode(self.payload_content_base64)
        except Exception as e:
            logger.error("Invalid Base64 content: %s", str(e))
            raise HTTPException(status_code=400, detail="Invalid Base64 content.")


        # Read the CSV content from the decoded Base64 string
        try:
            df=pd.read_csv(StringIO(decoded_content.decode()))
        except Exception as e:
            logger.error("Failed to parse CSV content: %s", str(e))
            raise HTTPException(status_code=400, detail="Failed to parse CSV content.")

        # Parse the JSON schema
        try:
            schema = json.loads(self.payload_schema)
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in payload schema.")
            raise HTTPException(status_code=400, detail="Invalid JSON format in payload schema")


        #Validate the data against the schema
        errors = self.validate_data(df,schema)
        if errors:
            return {"status" : "error", "errors": errors}
        else:
            return {"status" : "success", "message" : "Data is valid!" }


    # Function to validate the data based on the schema
    def validate_data(self, df, schema):
        """
             Validates DataFrame columns against the schema.
             DataFrame containing data to validate.
             JSON schema for expected column types.

             Returns:
             -------
             list
                 List of errors found during validation.
        """

        errors =[]
        for column , expected_type in schema.items():
            if column not in df.columns:
                errors.append(f"Missing columns: {column}")
            else:
                actual_type= str(df[column].dtype)
                if actual_type != expected_type:
                    errors.append(f"Incorrect type for {column}. Expected {expected_type}, but got {actual_type}")
        return errors
