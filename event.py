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


# Initializing
# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

# Get api fixture id, store it as a variable
#fixtureId = int(input("Enter the fixture ID:  "))
fixtureId = 147926
######fixtureId = 147915
#fixtureId = 147936
# Store path to fixture info in a variable, to be used w/ connection information
path = f"/fixtures/events?fixture={fixtureId}"

# Get api info on fixture, store it as a variable, payload
apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
apiconn.request("GET", path, headers=headers)
res = apiconn.getresponse()
raw = res.read()
payload = json.loads(raw.decode("utf-8"))
print(payload)
# Strip out just the fixture info
# fixture = ""
# leagueinfo = ""
# for item in payload.get("response", []):
#     fixture = item.get("fixture") or {}
#     leagueinfo = item.get("league") or {}
#     teamsinfo = item.get("teams") or {}
#     goalsinfo = item.get("goals") or {}
#     scoreinfo = item.get("score") or {}
# apiconn.close()
# print(fixture)

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

# Check to see if the fixture has events already in the table

# Grab the api fixtureid



# Grab the database fixtureid

# Event type work

# Event comments

# Time elapsed

# Extratime elapsed

# Get database team id

# Get database player id

# Assist work (database player id)

# Load into database