# Python Script
import datetime
import json
import psycopg2


def loaddbconfig(configPath="dbconfig.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


with open('Archive/2021attendance.txt', 'r') as file:
    lines = file.readlines()

updates = []
for line in lines:
    date_part, attendance = line.strip().split(': ')
    # Convert date to SQL date format (just the date, not timestamp)
    date = datetime.datetime.strptime(date_part, '%m/%d/%y').strftime('%Y-%m-%d')
    updates.append((date, int(attendance)))

# Execute the SQL
print("Loading DB config...")
db = loaddbconfig("dbconfig.json")

print("Connecting to database...")
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

print("\n" + "=" * 50)
print("DEBUGGING: Checking existing dates in fixture table...")
print("=" * 50 + "\n")

with conn.cursor() as cur:
    # Check what dates exist in the database
    cur.execute("SELECT atlantatime FROM fixture ORDER BY atlantatime LIMIT 10")
    sample_dates = cur.fetchall()
    print("Sample dates from database:")
    for row in sample_dates:
        print(f"  {row[0]} (type: {type(row[0])})")

    print("\nDates from your text file:")
    for date, att in updates[:5]:
        print(f"  {date} -> {att}")

print("\n" + "=" * 50)
print("EXECUTING UPDATES...")
print("=" * 50 + "\n")

# Use parameterized queries instead of string formatting (safer and handles types better)
total_updated = 0
with conn:
    with conn.cursor() as cur:
        for date, attendance in updates:
            # Use DATE cast to match just the date part, ignoring time
            cur.execute("""
                        UPDATE fixture
                        SET attendance = %s, updated_by = 'gislobo'
                        WHERE DATE(atlantatime) = %s
                        """, (attendance, date))

            rows_affected = cur.rowcount
            if rows_affected > 0:
                print(f"✓ Updated {rows_affected} row(s) for date {date} with attendance {attendance}")
                total_updated += rows_affected
            else:
                print(f"⚠ No rows found for date {date}")

print("\n" + "=" * 50)
print(f"✓ Successfully updated {total_updated} total rows!")
print("=" * 50)

conn.close()
print("\nDone!")