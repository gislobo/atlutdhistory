import json
import http.client
import sys
import psycopg2
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def loadheaders(headersPath="headers.json"):
    with open(headersPath, "r", encoding="utf-8") as f:
        return json.load(f)


def loaddbconfig(configPath="dbconfig.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return{
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


def _parse_api_utc(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def to_tz_from_utc(utc_dt):
    """
    Convert a UTC datetime (str or datetime) to the given IANA timezone.

    - utc_dt can be:
      - ISO 8601 string (e.g., "2025-09-27T14:30:00Z" or "2025-09-27T14:30:00+00:00")
      - naive datetime assumed to be UTC
      - aware datetime (will be converted from its timezone to target)

    Returns an aware datetime in the target timezone. If the requested timezone
    cannot be loaded on this system, falls back to the local timezone.
    """
    target_tz = "America/New_York"
    if isinstance(utc_dt, str):
        s = utc_dt.strip()
        if not s:
            raise ValueError("utc_dt string is empty")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception as e:
            raise ValueError(f"Unable to parse datetime string: {utc_dt!r}") from e
    elif isinstance(utc_dt, datetime):
        dt = utc_dt
    else:
        raise TypeError("utc_dt must be a str or datetime")

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
        print(f"naive testing, dt is {dt}")


    # Convert string timezone to ZoneInfo object
    try:
        tz = ZoneInfo(target_tz)
    except Exception:
        # Fallback to local timezone if ZoneInfo fails
        return dt.astimezone()

    return dt.astimezone(tz)


## Initializing
# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadheaders("headers.json")
print("...headers loaded.")
print("")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loaddbconfig("dbconfig.json")
print("...DB config loaded.")
print("")

# Get what season you want the fixture list for
print("Getting the season...")
#season = int(input("Enter the season:  "))
season = 2019
print(f"...season is {season}.")
print("Storing the path to the api in a path variable...")
path = f"/fixtures?team=1608&season={season}"
print("...path stored.")
print("")

# Get api info on fixture, store it as a variable, payload
print("Making the request to the api...")
apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
apiconn.request("GET", path, headers=headers)
res = apiconn.getresponse()
raw = res.read()
payload = json.loads(raw.decode("utf-8"))
print(payload)
print("...done, and raw payload data stored.")
print("")

## Connect once to postgres for lookups and load
print("Establishing connection to the database...")
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)
print("...connection established.")
print("")

## Get the response into a list of dictionaries and comparing length to results
# Get the number of results the api tells us there are
results = payload.get("results")
print(f"The API tells us there are {results} results.")
print("Getting the response into a list of dictionaries...")
response = payload.get("response") or {}
print("Making sure the number of results matches the number of responses...")
if len(response) != results:
    print("Something is wrong, the number of responses doesn't match the number of results the API tells us there are.")
    sys.exit(0)
print(f"There are {len(response)} responses in the payload, everything is good.")
print(response)

## Loop through each fixture and get api fixutre id and the date
print("Looping through each fixture and getting the fixture id and the date...")
count = 0
for fixture in response:
    count += 1
    print(f"Fixture {count}:")
    print(fixture)

    # Getting fixture info
    fixtureinfo = fixture.get("fixture") or {}
    apifixtureid = fixtureinfo.get("id")
    print(f"The api fixture id is {apifixtureid}.")

    fixturedateaware = fixtureinfo.get("date")
    print(f"The fixture date is {fixturedateaware} utc time.")
    atlantatimeaware = to_tz_from_utc(fixturedateaware)
    print(f"The fixture date is {atlantatimeaware} atlanta time.")
    # For timestamp (without time zone) columns, use naive "wall times"
    fixturedate = atlantatimeaware.replace(tzinfo=None)
    print(f"The fixture date is {fixturedate} atlanta time.")

    # Insert fixture into table
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.apifixturelist (apifixtureid, atlantatime) VALUES (%s, %s)",
                (apifixtureid, fixturedate),
            )
            print(f"Fixture has been inserted into the database.")
            print("")