"""
This script is used to setup the Azurite emulator for testing purposes. 
It creates a container in Azurite and uploads a geojson file to the container.
"""
import os

from azure.storage.blob import BlobServiceClient


def main():
    # Define connection string, container name, and geojson filename
    conn_str = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"

    # Get current path
    current_path = os.getcwd()

    # Create azurite connection
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Create a container
    container_name = "test-container"
    blob_service_client.create_container(container_name)

    # Upload geojson to the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.geojson")
    with open(os.path.join(current_path, "tests/data/test.geojson"), "rb") as data:
        blob_client.upload_blob(data)

    # Upload csv to the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.csv")
    with open(os.path.join(current_path, "tests/data/test.csv"), "rb") as data:
        blob_client.upload_blob(data)
    
    # Upload parquet to the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.parquet")
    with open(os.path.join(current_path, "tests/data/test.parquet"), "rb") as data:
        blob_client.upload_blob(data)
    
    # Upload geoparquet to the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.geoparquet")
    with open(os.path.join(current_path, "tests/data/test.geoparquet"), "rb") as data:
        blob_client.upload_blob(data)

if __name__ == "__main__":
    main()