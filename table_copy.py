import json
import psycopg2
import psycopg2.extras
from typing import List, Tuple, Dict


def load_config(config_path: str = "table_copy_config.json") -> Dict:
    """
    Load configuration from JSON file.

    Expected JSON structure:
    {
        "source": {
            "host": "localhost",
            "port": 5432,
            "database": "source_db",
            "user": "postgres",
            "password": "password",
            "table": "source_table"
        },
        "target": {
            "host": "localhost",
            "port": 5432,
            "database": "target_db",
            "user": "postgres",
            "password": "password",
            "table": "target_table"
        },
        "options": {
            "truncate_target": true,
            "batch_size": 1000
        }
    }
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def export_table_data(
        config: Dict,
        table: str
) -> Tuple[List[str], List[Tuple]]:
    """
    Export data from a PostgreSQL table.

    Args:
        config: Database connection configuration
        table: Table name to export from

    Returns:
        Tuple containing column names and row data
    """
    conn = None
    try:
        # Connect to source database
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            database=config.get("database"),
            user=config.get("user"),
            password=config.get("password"),
            port=config.get("port", 5432)
        )
        cursor = conn.cursor()

        # Fetch all data from the table
        cursor.execute(f"SELECT * FROM {table}")

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Fetch all rows
        rows = cursor.fetchall()

        print(f"Exported {len(rows)} rows from {table}")
        return column_names, rows

    except Exception as e:
        print(f"Error exporting data: {e}")
        raise
    finally:
        if conn:
            conn.close()


def import_table_data(
        config: Dict,
        table: str,
        column_names: List[str],
        rows: List[Tuple],
        batch_size: int = 1000
) -> None:
    """
    Import data into a PostgreSQL table.

    Args:
        config: Database connection configuration
        table: Table name to import into
        column_names: List of column names
        rows: List of row tuples
        batch_size: Number of rows to insert per batch
    """
    conn = None
    try:
        # Connect to target database
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            database=config.get("database"),
            user=config.get("user"),
            password=config.get("password"),
            port=config.get("port", 5432)
        )
        cursor = conn.cursor()

        # Build INSERT query
        columns_str = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        # Insert data in batches
        total_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            psycopg2.extras.execute_batch(cursor, insert_query, batch)
            total_inserted += len(batch)
            print(f"Inserted {total_inserted}/{len(rows)} rows")

        # Commit the transaction
        conn.commit()
        print(f"Successfully imported {len(rows)} rows into {table}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error importing data: {e}")
        raise
    finally:
        if conn:
            conn.close()


def copy_table_data(config_path: str = "table_copy_config.json") -> None:
    """
    Copy data from one PostgreSQL table to another using configuration file.

    Args:
        config_path: Path to the JSON configuration file
    """
    try:
        # Load configuration
        print(f"Loading configuration from {config_path}...")
        config = load_config(config_path)
        print("Configuration loaded successfully.")

        source_config = config.get("source", {})
        target_config = config.get("target", {})
        options = config.get("options", {})

        source_table = source_config.get("table")
        target_table = target_config.get("table")
        truncate_target = options.get("truncate_target", False)
        batch_size = options.get("batch_size", 1000)

        # Validate configuration
        if not source_table or not target_table:
            raise ValueError("Source and target table names must be specified in config")

        # Optionally truncate target table
        if truncate_target:
            conn = psycopg2.connect(
                host=target_config.get("host", "localhost"),
                database=target_config.get("database"),
                user=target_config.get("user"),
                password=target_config.get("password"),
                port=target_config.get("port", 5432)
            )
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {target_table}")
            conn.commit()
            conn.close()
            print(f"Truncated target table {target_table}")

        # Export data from source
        print(f"Exporting data from {source_table}...")
        column_names, rows = export_table_data(source_config, source_table)

        # Import data to target
        if rows:
            print(f"Importing data to {target_table}...")
            import_table_data(target_config, target_table, column_names, rows, batch_size)
        else:
            print("No data to import")

        print("Table copy completed successfully!")

    except Exception as e:
        print(f"Error copying table data: {e}")
        raise


if __name__ == "__main__":
    # You can specify a different config file path if needed
    copy_table_data("table_copy_config.json")