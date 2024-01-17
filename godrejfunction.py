import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient

def move_files_to_raw_data(data, context):
    # Set your Azure Storage connection string
    storage_connection_string = "BlobEndpoint=https://godrejacc.blob.core.windows.net/;QueueEndpoint=https://godrejacc.queue.core.windows.net/;FileEndpoint=https://godrejacc.file.core.windows.net/;TableEndpoint=https://godrejacc.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-17T20:22:46Z&st=2024-01-17T12:22:46Z&spr=https&sig=BpPQhXCzmKp%2BXDlxH%2BocT%2Bwn1EaTzI9NCoBcuK87hGs%3D"

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get the container clients
    upload_data_container_name = "upload-data-container"
    raw_data_container_name = "raw-data-container"
    
    upload_data_container_client = blob_service_client.get_container_client(upload_data_container_name)
    raw_data_container_client = blob_service_client.get_container_client(raw_data_container_name)

    # Get the list of blobs in the upload-data-container
    blobs = upload_data_container_client.list_blobs()

    # Get the timestamp for the current time
    timestamp = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

    # Iterate over each blob
    for blob in blobs:
        # Check if the blob is a CSV file
        if blob.name.endswith('.csv'):
            # Create new blob names for archive and raw data
            archive_blob_name = f"Archive_to_Server/{blob.name}_{timestamp}"
            raw_data_blob_name = f"data/source_files/{blob.name}"

            # Copy the blob to Archive container
            archive_blob_client = raw_data_container_client.get_blob_client(archive_blob_name)
            archive_blob_client.start_copy_from_url(blob.url)

            # Copy the blob to Raw Data container
            raw_data_blob_client = raw_data_container_client.get_blob_client(raw_data_blob_name)
            raw_data_blob_client.start_copy_from_url(blob.url)

            # Delete the blob from Upload Data container
            upload_data_container_client.delete_blob(blob.name)

    return "Function execution completed."


