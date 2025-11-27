# test_adls_write_env.py
import os
from azure.storage.filedatalake import DataLakeServiceClient

# Read connection from environment
ADLS_CONNECTION_STRING = os.getenv("ADLS_CONNECTION_STRING")
ADLS_CONTAINER_NAME = os.getenv("ADLS_CONTAINER_NAME")

def test_adls_write():
    try:
        service_client = DataLakeServiceClient.from_connection_string(ADLS_CONNECTION_STRING)
        filesystem_client = service_client.get_file_system_client(ADLS_CONTAINER_NAME)

        # Create a test file
        directory_client = filesystem_client.get_directory_client("test")  # folder 'test'
        file_client = directory_client.create_file("test_file.txt")

        content = "Hello ADLS! This is a test."
        file_client.append_data(content, 0, len(content))
        file_client.flush_data(len(content))

        print(f"✅ Successfully wrote test_file.txt to {ADLS_CONTAINER_NAME}/test/")
    except Exception as e:
        print(f"❌ Failed to write to ADLS: {e}")

if __name__ == "__main__":
    test_adls_write()

