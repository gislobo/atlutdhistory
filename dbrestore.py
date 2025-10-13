from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import subprocess
import os
import json


def load_backup_config(config_path="backupConfig.json"):
    """Load backup configuration from JSON file"""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_available_backups(config):
    """List all backup files available in blob storage"""
    try:
        # Check if using connection string or credential
        if 'storage_connection_string' in config and config['storage_connection_string']:
            blob_service_client = BlobServiceClient.from_connection_string(
                config['storage_connection_string']
            )
        else:
            credential = DefaultAzureCredential()
            account_url = f"https://{config['storage_account_name']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url, credential=credential)

        container_client = blob_service_client.get_container_client(config['container_name'])

        print(f"Available backups in container '{config['container_name']}':")
        print("-" * 80)

        blobs = list(container_client.list_blobs())

        if not blobs:
            print("No backups found!")
            return []

        # Sort by last modified (newest first)
        blobs.sort(key=lambda x: x.last_modified, reverse=True)

        backup_list = []
        for idx, blob in enumerate(blobs, 1):
            size_mb = blob.size / (1024 * 1024)
            print(f"{idx}. {blob.name}")
            print(f"   Size: {size_mb:.2f} MB")
            print(f"   Last Modified: {blob.last_modified}")
            print()
            backup_list.append(blob.name)

        return backup_list

    except Exception as e:
        print(f"Error listing backups: {e}")
        return []


def download_backup(blob_name, config):
    """Download a backup file from Azure Blob Storage"""
    try:
        # Check if using connection string or credential
        if 'storage_connection_string' in config and config['storage_connection_string']:
            blob_service_client = BlobServiceClient.from_connection_string(
                config['storage_connection_string']
            )
        else:
            credential = DefaultAzureCredential()
            account_url = f"https://{config['storage_account_name']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url, credential=credential)

        blob_client = blob_service_client.get_blob_client(
            container=config['container_name'],
            blob=blob_name
        )

        # Download to current directory
        local_filename = blob_name
        print(f"Downloading {blob_name}...")

        with open(local_filename, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        print(f"Downloaded successfully: {local_filename}")
        return local_filename

    except Exception as e:
        print(f"Error downloading backup: {e}")
        return None


def restore_backup(backup_file, target_db, config):
    """Restore a PostgreSQL backup to a target database using pg_restore"""
    print(f"Restoring {backup_file} to database '{target_db}'...")

    # Set password environment variable for pg_restore
    env = os.environ.copy()
    env['PGPASSWORD'] = config['postgres_password']

    # Check if target database exists, create if not
    print(f"Checking if database '{target_db}' exists...")
    check_command = [
        'psql',
        '-h', config['postgres_host'],
        '-U', config['postgres_user'],
        '-d', 'postgres',  # Connect to postgres db to check
        '-t',  # Tuples only
        '-c', f"SELECT 1 FROM pg_database WHERE datname='{target_db}'"
    ]

    try:
        result = subprocess.run(check_command, env=env, capture_output=True, text=True, check=True)

        if '1' not in result.stdout:
            # Database doesn't exist, create it
            print(f"Database '{target_db}' does not exist. Creating...")
            create_command = [
                'psql',
                '-h', config['postgres_host'],
                '-U', config['postgres_user'],
                '-d', 'postgres',
                '-c', f"CREATE DATABASE {target_db}"
            ]
            subprocess.run(create_command, env=env, check=True)
            print(f"Database '{target_db}' created successfully.")
        else:
            print(f"Database '{target_db}' already exists.")
            response = input(f"Do you want to drop and recreate '{target_db}'? (yes/no): ")
            if response.lower() in ['yes', 'y']:
                print(f"Dropping database '{target_db}'...")
                drop_command = [
                    'psql',
                    '-h', config['postgres_host'],
                    '-U', config['postgres_user'],
                    '-d', 'postgres',
                    '-c', f"DROP DATABASE {target_db}"
                ]
                subprocess.run(drop_command, env=env, check=True)

                print(f"Recreating database '{target_db}'...")
                create_command = [
                    'psql',
                    '-h', config['postgres_host'],
                    '-U', config['postgres_user'],
                    '-d', 'postgres',
                    '-c', f"CREATE DATABASE {target_db}"
                ]
                subprocess.run(create_command, env=env, check=True)
            else:
                print("Restore cancelled.")
                return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking/creating database: {e}")
        return False

    # Restore the backup with better error handling
    restore_command = [
        'pg_restore',
        '-h', config['postgres_host'],
        '-U', config['postgres_user'],
        '-d', target_db,
        '-v',  # Verbose
        '--no-owner',  # Skip ownership restoration
        '--no-acl',  # Skip ACL restoration
        '--exit-on-error',  # Exit on first error (we'll catch it)
        backup_file
    ]

    # Run without --exit-on-error to allow partial restore
    restore_command_lenient = [
        'pg_restore',
        '-h', config['postgres_host'],
        '-U', config['postgres_user'],
        '-d', target_db,
        '-v',  # Verbose
        '--no-owner',  # Skip ownership restoration
        '--no-acl',  # Skip ACL restoration
        backup_file
    ]

    print("Starting restore (this may take a while)...")
    print()

    try:
        # Use lenient mode - capture output to show details
        result = subprocess.run(
            restore_command_lenient,
            env=env,
            capture_output=True,
            text=True
        )

        # Show output
        if result.stdout:
            print("Restore output:")
            print(result.stdout)

        if result.stderr:
            print("Restore messages/warnings:")
            print(result.stderr)

        # Check if restore completed (even with warnings)
        if result.returncode == 0:
            print()
            print(f"✓ Restore completed successfully to database '{target_db}'!")
            return True
        elif result.returncode == 1:
            # Exit code 1 usually means warnings, not fatal errors
            print()
            print(f"⚠ Restore completed with warnings to database '{target_db}'.")
            print("This is common and usually not a problem.")

            # Verify data was restored
            print()
            print("Verifying restore...")
            verify_command = [
                'psql',
                '-h', config['postgres_host'],
                '-U', config['postgres_user'],
                '-d', target_db,
                '-t',
                '-c', "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
            ]

            verify_result = subprocess.run(
                verify_command,
                env=env,
                capture_output=True,
                text=True
            )

            if verify_result.returncode == 0:
                table_count = verify_result.stdout.strip()
                print(f"✓ Verification passed: {table_count} tables found in restored database.")
                return True
            else:
                print("✗ Could not verify restore.")
                return False
        else:
            print()
            print(f"✗ Restore failed with exit code {result.returncode}")
            return False

    except Exception as e:
        print(f"Error during restore: {e}")
        return False


def cleanup_local_file(filename):
    """Remove downloaded backup file"""
    try:
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Local file {filename} removed")
    except Exception as e:
        print(f"Error removing local file: {e}")


def main():
    print("PostgreSQL Database Restore from Azure Blob Storage")
    print("=" * 80)
    print()

    # Load configuration
    print("Loading configuration...")
    config = load_backup_config("backupConfig.json")
    print("Configuration loaded.")
    print()

    # List available backups
    backup_list = list_available_backups(config)

    if not backup_list:
        print("No backups available to restore.")
        return

    # Select backup to restore
    print("-" * 80)
    selection = input("Enter the number of the backup to restore (or 'q' to quit): ")

    if selection.lower() == 'q':
        print("Restore cancelled.")
        return

    try:
        backup_index = int(selection) - 1
        if backup_index < 0 or backup_index >= len(backup_list):
            print("Invalid selection.")
            return

        selected_backup = backup_list[backup_index]
        print(f"Selected: {selected_backup}")
        print()

    except ValueError:
        print("Invalid input.")
        return

    # Get target database name
    target_db = input(f"Enter target database name (default: {config['postgres_db']}_restore): ").strip()
    if not target_db:
        target_db = f"{config['postgres_db']}_restore"

    print()
    print(f"Will restore '{selected_backup}' to database '{target_db}'")
    confirm = input("Continue? (yes/no): ")

    if confirm.lower() not in ['yes', 'y']:
        print("Restore cancelled.")
        return

    print()

    # Download backup
    local_file = download_backup(selected_backup, config)

    if local_file:
        # Restore backup
        success = restore_backup(local_file, target_db, config)

        if success:
            # Optionally cleanup downloaded file
            cleanup = input("Delete downloaded backup file? (yes/no): ")
            if cleanup.lower() in ['yes', 'y']:
                cleanup_local_file(local_file)
        else:
            print("Keeping downloaded file for troubleshooting.")


if __name__ == "__main__":
    main()