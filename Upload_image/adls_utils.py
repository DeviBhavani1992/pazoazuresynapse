import os
import json
import logging
from azure.storage.filedatalake import DataLakeServiceClient

ADLS_CONNECTION_STRING = os.getenv("ADLS_CONNECTION_STRING")
ADLS_CONTAINER_NAME = os.getenv("ADLS_CONTAINER_NAME", "pazo")


def get_service_client():
    """Create ADLS service client"""
    if not ADLS_CONNECTION_STRING:
        raise ValueError("ADLS_CONNECTION_STRING must be set")

    return DataLakeServiceClient.from_connection_string(ADLS_CONNECTION_STRING)


def ensure_container_exists(service_client):
    """Ensure the ADLS container exists; create if missing"""
    try:
        container = service_client.get_file_system_client(ADLS_CONTAINER_NAME)
        container.create_file_system()
        logging.info(f"Container created: {ADLS_CONTAINER_NAME}")
    except Exception:
        # Container may already exist – safe to ignore
        pass

    return container


def upload_json_to_adls(data, category, timestamp, store_id):
    try:
        logging.info(f"🚀 Starting ADLS upload for {category}_{timestamp}")

        # Create ADLS client
        service_client = get_service_client()

        # Ensure container exists
        file_system_client = ensure_container_exists(service_client)

        # Extract folder structure from timestamp
        date = timestamp[:8]  # YYYYMMDD
        year, month, day = date[:4], date[4:6], date[6:8]

        # Define ADLS path
        directory_path = f"pazo/{store_id}/{year}/{month}/{day}"
        file_path = f"{directory_path}/{category}_{timestamp}.json"

        logging.info(f"📁 Directory Path: {directory_path}")
        logging.info(f"📄 File Path: {file_path}")

        # Create directory safely
        try:
            dir_client = file_system_client.get_directory_client(directory_path)
            dir_client.create_directory()
            logging.info("📁 Directory created successfully")
        except Exception as e:
            logging.info(f"ℹ️ Directory already exists or skipped: {str(e)}")

        # Convert JSON data to bytes
        json_data = json.dumps(data, indent=2)
        json_bytes = json_data.encode("utf-8")

        # Upload the file
        file_client = file_system_client.get_file_client(file_path)

        logging.info("⬆️ Uploading JSON data to ADLS...")
        file_client.create_file()
        file_client.append_data(json_bytes, offset=0, length=len(json_bytes))
        file_client.flush_data(len(json_bytes))

        logging.info(f"✅ Successfully uploaded → {file_path}")

        return file_path

    except Exception as e:
        logging.error(f"❌ ADLS upload failed: {str(e)}")
        raise Exception(f"Failed to upload to ADLS: {str(e)}")

