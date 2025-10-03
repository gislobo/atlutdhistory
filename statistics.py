import json
import http.client
import sys
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


## Initializing
# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

## Get api fixture id, store it as a variable
#apifixtureid = int(input("Enter the fixture ID:  "))
apifixtureid = 147926
######fixtureId = 147915
#apifixtureid = 147936
# Store path to fixture info in a variable, to be used w/ connection information
path = f"/fixtures/statistics?fixture={apifixtureid}"

## Get api info on fixture, store it as a variable, payload
apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
apiconn.request("GET", path, headers=headers)
res = apiconn.getresponse()
raw = res.read()
payload = json.loads(raw.decode("utf-8"))
print(payload)

## Connect once to postgres for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

## Grab the database fixtureid
with conn.cursor() as cur:
    cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (apifixtureid,))
    existingfixtures = cur.fetchall()
existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if existingfixture[0] is not None}
print(f"Existing fixtures: {existingfixturesdict}")
databasefixtureid = ""
if apifixtureid in existingfixturesdict:
    databasefixtureid = existingfixturesdict[apifixtureid]
    print(f"The database fixture id is {databasefixtureid}.")

## Work out how to grab each team's statistics individually
# API tells us how many events there are
apiresults = payload.get("results") or {}
print(f"The API tells us there are {apiresults} events.")

# Ge the events into a list of dictionaries
response = payload.get("response") or {}
print(response)
print(f"There are {len(response)} events in the response.")
if len(response) == apiresults:
    print("The number of events in the response matches the number of events the API initially tells us there are.")
else:
    print("Something is wrong, the number of events in the response doesn't match the number of events the API tells us there are.")
    sys.exit(0)

count = 0
for event in response:
    count += 1
    print(f"Event {count}:")
    print(event)

    ## Get db team id

    ## Get stats into variables
    apistats = event.get("statistics") or {}
    print(f"Stats: {apistats}")
    print(f"length of stats: {len(apistats)}")
    for stat in apistats:
        print(f"stat: {stat}")
        stattype = stat.get("type")
        statvalue = stat.get("value")
        if stattype == 'Passes %':
            print(f"passing accuracy: {statvalue}")
            print(type(statvalue))