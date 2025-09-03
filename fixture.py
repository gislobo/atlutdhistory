import json
import http.client
import psycopg2

def loadHeaders(headersPath="headers.json"):
    with open(headersPath, "r", encoding="utf-8") as f:
        return json.load(f)


def loadDbConfig(configPath="dbConfig.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return{
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


def refereeWork(f, conn):
    # Get the raw referee info
    refereeRaw = f.get("referee")
    # If there is no information for referee in api, setting variables for None
    referee = None
    refereeCountry = None
    refId = 1
    if refereeRaw:
        # Strip it into parts and store as variables
        parts = [p.strip() for p in refereeRaw.split(",")]
        referee = parts[0] if len(parts) > 0 and parts[0] else None
        refereeCountry = parts[1] if len(parts) > 1 and parts[1] else None
        print(f"Referee: {referee}, Country: {refereeCountry}")
        # See if referee is in db
        with conn.cursor() as cur:
            cur.execute("SELECT concat_ws(' ', firstname, lastname) as fullname, id FROM public.referee")
            rows = cur.fetchall()
        existingReferees = {row[0]: row[1] for row in rows if row[0] is not None}
        print(existingReferees)
        # If referee is in db, get referee id
        if referee in existingReferees:
            refId = existingReferees[referee]
            print(f"Referee {referee} is already in the database, referee id: {refId}")
        ##if referee is not in db, add referee to db

    else:
        print("No referee information, referee will be \'None\'.")

    print(f"refId = {refId}.")




# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

# Get fixture id, store it as a variable
#fixtureId = int(input("Enter the fixture ID:  "))
#fixtureId = 147926
#fixtureId = 147915
fixtureId = 147936
# Store path to fixture info in a variable, to be used w/ connection information
path = f"/fixtures?id={fixtureId}"

# Get api info on fixture, store it as a variable, payload
apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
apiconn.request("GET", path, headers=headers)
res = apiconn.getresponse()
raw = res.read()
payload = json.loads(raw.decode("utf-8"))
# Strip out just the fixture info
fixture = ""
for item in payload.get("response", []):
    fixture = item.get("fixture") or {}
apiconn.close()
print(fixture)

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

# Looking into the referee info
refereeWork(fixture, conn)

