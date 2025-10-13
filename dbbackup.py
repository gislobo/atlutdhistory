from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import subprocess
import os
import json
from datetime import datetime


def load_backup_config(config_path="backupconfig.json"):
    """Load backup configuration from JSON file"""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def create_backup(config):
    """Create a PostgreSQL backup using pg_dump"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Changed extension to .dump for clarity
    backup_filename = f"backup_{config['postgres_db']}_{timestamp}.dump"

    print(f"Creating backup: {backup_filename}")

    # Set password environment variable for pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = config['postgres_password']

    # Run pg_dump with custom format (compressed, flexible)
    command = [
        'pg_dump',
        '-h', config['postgres_host'],
        '-U', config['postgres_user'],
        '-d', config['postgres_db'],
        '-F', 'c',  # Custom format (compressed, best for cloud storage)
        '-b',  # Include large objects
        '-v',  # Verbose mode
        '-f', backup_filename
    ]

    try:
        subprocess.run(command, env=env, check=True)
        print(f"Backup created successfully: {backup_filename}")
        return backup_filename
    except subprocess.CalledProcessError as e:
        print(f"Error creating backup: {e}")
        return None


def upload_to_blob_storage(local_file, config):
    """Upload backup file to Azure Blob Storage"""
    try:
        # Authenticate using DefaultAzureCredential
        credential = DefaultAzureCredential()

        # Create BlobServiceClient
        account_url = f"https://{config['storage_account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url, credential=credential)

        # Get container client (create container if it doesn't exist)
        container_client = blob_service_client.get_container_client(config['container_name'])
        try:
            container_client.create_container()
            print(f"Container '{config['container_name']}' created")
        except Exception:
            print(f"Container '{config['container_name']}' already exists")

        # Upload file
        blob_name = os.path.basename(local_file)
        blob_client = blob_service_client.get_blob_client(
            container=config['container_name'],
            blob=blob_name
        )

        print(f"Uploading {local_file} to blob storage...")
        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"Successfully uploaded to: {blob_name}")
        return True

    except Exception as e:
        print(f"Error uploading to blob storage: {e}")
        return False


def cleanup_local_backup(filename):
    """Remove local backup file after upload"""
    try:
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Local backup file {filename} removed")
    except Exception as e:
        print(f"Error removing local file: {e}")


def main():
    print("Starting PostgreSQL backup process...")
    print("")

    # Load configuration
    print("Loading backup configuration...")
    config = load_backup_config("backupConfig.json")
    print("...backup configuration loaded.")
    print("")

    # Create backup
    backup_file = create_backup(config)

    if backup_file:
        # Upload to blob storage
        if upload_to_blob_storage(backup_file, config):
            print("Backup completed successfully!")
            # Optionally cleanup local file
            if config.get('cleanup_local_backup', True):
                cleanup_local_backup(backup_file)
        else:
            print("Failed to upload backup")
    else:
        print("Failed to create backup")


if __name__ == "__main__":
    main()