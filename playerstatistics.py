import json
import http.client
import sys
import psycopg2

def loadheaders(headersPath="headers.json"):
    with open(headersPath, "r", encoding="utf-8") as f:
        return json.load(f)


def loaddbconfig(configPath="dbConfig.json"):
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
headers = loadheaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loaddbconfig("dbConfig.json")
print("...DB config loaded.")

## Get api fixture id, store it as a variable
print("Getting the api fixture id...")
#apifixtureid = int(input("Enter the fixture ID:  "))
apifixtureid = 147926
######fixtureId = 147915
#apifixtureid = 147936
print(f"...fixture id is {apifixtureid}.")
# Store path to fixture info in a variable, to be used w/ connection information
print("Storing the path to the api in a path variable...")
path = f"/fixtures/statistics?fixture={apifixtureid}"
print("...path stored.")
print("")

## Get api info on fixture, store it as a variable, payload
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

## Grab the database fixtureid
print("Grabbing the database fixture id...")
with conn.cursor() as cur:
    cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (apifixtureid,))
    existingfixtures = cur.fetchall()
existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if existingfixture[0] is not None}
databasefixtureid = None
if apifixtureid in existingfixturesdict:
    databasefixtureid = existingfixturesdict[apifixtureid]
    print(f"The database fixture id is {databasefixtureid}.")
    print("")

## See if the fixture already has player statistics
print("Checking if the fixture already has player statistics in the database...")
with conn.cursor() as cur:
    cur.execute("select dbfixtureid from public.fixtureplayerstatistics")
    existingfixturestatisticsidsfetchall = cur.fetchall()
existingfixturestatisticsids = {row[0] for row in existingfixturestatisticsidsfetchall}
if databasefixtureid in existingfixturestatisticsids:
    print(f"The fixture {databasefixtureid} already has statistics in the database, exiting.")
    sys.exit(0)
print("...fixture does not have statistics in the database, proceeding.")
print("")
