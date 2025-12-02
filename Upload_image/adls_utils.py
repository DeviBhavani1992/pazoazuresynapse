import os
import json
import logging
from azure.storage.filedatalake import DataLakeServiceClient
import posixpath 

# ------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------

ADLS_CONNECTION_STRING = os.getenv("ADLS_CONNECTION_STRING")
ADLS_CONTAINER_NAME = os.getenv("ADLS_CONTAINER_NAME", "pazo")


# ------------------------------
# GET SERVICE CLIENT
# ------------------------------

def get_service_client():
    """Create ADLS service client"""
    if not ADLS_CONNECTION_STRING:
        raise ValueError("ADLS_CONNECTION_STRING environment variable is missing")

    return DataLakeServiceClient.from_connection_string(ADLS_CONNECTION_STRING)


# ------------------------------
# ENSURE CONTAINER EXISTS
# ------------------------------

def ensure_container_exists(service_client):
    """Ensure the ADLS container exists; create if missing"""
    try:
        container = service_client.get_file_system_client(ADLS_CONTAINER_NAME)
        container.create_file_system()
        logging.info(f"Container created: {ADLS_CONTAINER_NAME}")
    except Exception:
        # Already exists, ignore
        pass

    return container


# ------------------------------
# CORE UPLOAD HELPER (Private Function)
# ------------------------------

def _upload_data_to_adls(file_system_client, directory_path, file_path, data, is_text):
    """Internal helper to upload bytes or text to a specified ADLS path."""
    
    # 1. Ensure Directory Exists
    try:
        dir_client = file_system_client.get_directory_client(directory_path)
        dir_client.create_directory()
        logging.debug(f"📁 Directory created or already exists: {directory_path}")
    except Exception:
        pass # Directory already exists

    # 2. Prepare Data
    if is_text:
        content_bytes = data.encode("utf-8")
    else:
        content_bytes = data # Assumes data is already bytes for raw content

    # 3. Upload File
    file_client = file_system_client.get_file_client(file_path)

    logging.info(f"⬆️ Uploading data to ADLS: {file_path}")
    file_client.create_file()
    file_client.append_data(content_bytes, offset=0, length=len(content_bytes))
    file_client.flush_data(len(content_bytes))

    logging.info(f"✅ Successfully uploaded → {file_path}")
    return file_path


# ------------------------------
# UPLOAD JSON TO ADLS
# ------------------------------
def upload_json_to_adls(data, category, timestamp, store_id):
    """Upload JSON result to ADLS path results/{store_id}/YYYY/MM/DD/category_timestamp.json"""

    try:
        service_client = get_service_client()
        file_system_client = ensure_container_exists(service_client)

        date = timestamp[:8]
        year, month, day = date[:4], date[4:6], date[6:8]

        # Path: results/{store_id}/YYYY/MM/DD/
        directory_path = posixpath.join("results", store_id, year, month, day)
        file_path = posixpath.join(directory_path, f"{category}_{timestamp}.json")

        json_string = json.dumps(data, indent=2)

        return _upload_data_to_adls(
            file_system_client, 
            directory_path, 
            file_path, 
            json_string, 
            is_text=True
        )

    except Exception as e:
        logging.error(f"❌ ADLS JSON upload failed: {str(e)}")
        raise Exception(f"Failed to upload JSON to ADLS: {str(e)}")


# ------------------------------
# UPLOAD IMAGE TO ADLS (Raw Content)
# ------------------------------
def upload_image_to_adls(file_content, category, store_id, original_filename, timestamp):
    """Uploads the raw image content (bytes) to ADLS path images/raw/..."""
    
    try:
        date = timestamp[:8]
        year, month, day = date[:4], date[4:6], date[6:8]
        _, file_extension = os.path.splitext(original_filename)
        
        directory_path = posixpath.join("images", "raw", store_id, year, month, day)
        file_path = posixpath.join(directory_path, f"{category}_{timestamp}{file_extension}")

        service_client = get_service_client()
        file_system_client = ensure_container_exists(service_client)
        
        return _upload_data_to_adls(
            file_system_client, 
            directory_path, 
            file_path, 
            file_content, 
            is_text=False
        )
    except Exception as e:
        logging.error(f"❌ ADLS raw image upload failed: {str(e)}")
        raise Exception(f"Failed to upload raw image to ADLS: {str(e)}")


# ------------------------------
# UPLOAD BASE64 TO ADLS (Text Content)
# ------------------------------
def upload_base64_to_adls(b64_string, category, store_id, timestamp):
    """Uploads the base64 string (text) to ADLS path images/base64/..."""
    
    try:
        date = timestamp[:8]
        year, month, day = date[:4], date[4:6], date[6:8]
        
        directory_path = posixpath.join("images", "base64", store_id, year, month, day)
        file_path = posixpath.join(directory_path, f"{category}_{timestamp}.txt")

        service_client = get_service_client()
        file_system_client = ensure_container_exists(service_client)
        
        return _upload_data_to_adls(
            file_system_client, 
            directory_path, 
            file_path, 
            b64_string, 
            is_text=True
        )
    except Exception as e:
        logging.error(f"❌ ADLS base64 upload failed: {str(e)}")
        raise Exception(f"Failed to upload base64 to ADLS: {str(e)}")


# ------------------------------
# ORCHESTRATOR: SAVE IMAGE AND BASE64
# ------------------------------
def save_image_and_base64(file_content, b64_string, category, store_id, original_filename, timestamp):
    """
    Saves the raw image content and its base64 string to ADLS using specific helpers.
    Returns the paths to both uploaded files.
    """
    
    raw_path = upload_image_to_adls(file_content, category, store_id, original_filename, timestamp)
    b64_path = upload_base64_to_adls(b64_string, category, store_id, timestamp)
    
    logging.info("✅ Both raw image and base64 text saved successfully.")
    
    return {"raw": raw_path, "base64": b64_path}