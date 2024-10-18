import base64
import requests
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""Send a POST request to validate data in CSV or JSON format.
file_path: Path to the file to validate
payload_format: Format of the payload (e.g., 'csv' or 'json')
payload_schema: JSON schema for data validation"""

def data_validate(file_path, payload_format, payload_schema):

    # Validate file existence
    if not os.path.exists(file_path):
        logger.error("File does not exist: %s", file_path)
        return {"status": "error", "detail": "File does not exist."}

    # Validate payload_format
    if payload_format.lower() not in {"csv", "json"}:
        logger.error("Unsupported payload_format: %s. Only 'csv' or 'json' are allowed.", payload_format)
        return {"status": "error", "detail": "Unsupported format. Only 'csv' or 'json' allowed."}

    # Validate payload_schema type
    if not isinstance(payload_schema, dict):
        logger.error("Invalid type for payload schema. Expected a dictionary, got %s.", type(payload_schema).__name__)
        return {"status": "error", "detail": "Invalid schema type. Expected a dictionary."}

    url = "http://127.0.0.1:8000/validate-data/"

    # Read and encode file content to Base64
    logger.info("Reading file: %s", file_path)
    try:
        with open(file_path, "rb") as f:
            file_content = f.read()
        logger.info("File read successfully!")
    except Exception as e:
        logger.error("Failed to read file: %s", e)
        return {"status": "error", "detail": f"Failed to read file: {str(e)}"}

    file_content_base64 = base64.b64encode(file_content).decode('utf-8')
    logger.info("Base64 encoding complete.")

    data = {
        "payload_format": payload_format,
        "payload_schema": json.dumps(payload_schema),
        "payload_content_base64": file_content_base64
    }

    # Send POST request to validate data
    try:
        logger.info("Sending POST request to %s", url)
        response = requests.post(url, json=data)

        if response.status_code == 200:
            result = response.json()
            if result["status"] == "success":
                print("Data is valid")
            else:
                logger.error("Validation errors: %s", result.get('errors'))
        else:
            logger.error("Failed to validate data. HTTP Status: %s", response.status_code)
            logger.error("Response: %s", response.text)
    except Exception as e:
        logger.error("An error occurred while sending the request: %s", e)
        return {"status": "error", "detail": f"Request failed: {str(e)}"}


# Example usage
if __name__ == "__main__":
    file_path = r"C:\Users\ishan\OneDrive\Desktop\testing.csv"
    payload_format = "csv"
    payload_schema = { }#{"Embarked": "object", "Age": "float64"}  # Ensure this is a dict, not a set
    data_validate(file_path, payload_format, payload_schema)
