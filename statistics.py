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


def percentstringtofloat(str):
    """Convert a percentage string like '61%' to a float like 61.0"""
    if str is None or str == "":
        return None
    return float(str.strip('%'))


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
apifixtureid = int(input("Enter the fixture ID:  "))
#apifixtureid = 147926
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
databasefixtureid = None
if apifixtureid in existingfixturesdict:
    databasefixtureid = existingfixturesdict[apifixtureid]
    print(f"The database fixture id is {databasefixtureid}.")

## See if the fixture already has statistics
with conn.cursor() as cur:
    cur.execute("select dbfixtureid from public.fixturestatistics")
    existingfixturestatisticsidsfetchall = cur.fetchall()
existingfixturestatisticsids = {row[0] for row in existingfixturestatisticsidsfetchall}
if databasefixtureid in existingfixturestatisticsids:
    print(f"The fixture {databasefixtureid} already has statistics in the database, exiting.")
    sys.exit(0)

## Work out how to grab each team's statistics individually
# API tells us how many events there are
apiresults = payload.get("results") or {}
print(f"The API tells us there are {apiresults} events.")

# Get the events into a list of dictionaries
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
    teaminfo = event.get("team") or {}
    apiteamid = teaminfo.get("id")
    print(f"apiteamid: {apiteamid}")
    with conn.cursor() as cur:
        cur.execute("SELECT apifootballid, id from public.team where apifootballid = %s", (apiteamid,))
        existingteams = cur.fetchall()
    existingteamsdict = {existingteam[0]: existingteam[1] for existingteam in existingteams if existingteam[0] is not None}
    databaseteamid = None
    if apiteamid in existingteamsdict:
        databaseteamid = existingteamsdict[apiteamid]
        print(f"The database team id is {databaseteamid}.")
    else:
        print(f"API Team ID {apiteamid} is not in your database.")
        sys.exit(0)

    ## Get stats into variables
    # Initialize variables
    shotsongoal = None
    shotsoffgoal = None
    totalshots = None
    blockedshots = None
    shotsinsidebox = None
    shotsoutsidebox = None
    fouls = None
    cornerkicks = None
    offsides = None
    ballpossessionstr = None
    ballpossession = None
    yellowcards = None
    redcards = None
    goalkeepersaves = None
    totalpasses = None
    passesaccurate = None
    apistats = event.get("statistics") or {}
    print(f"Stats: {apistats}")
    print(f"length of stats: {len(apistats)}")
    for stat in apistats:
        print(f"stat: {stat}")
        stattype = stat.get("type")
        statvalue = stat.get("value")
        if stattype == 'Shots on Goal':
            shotsongoal = statvalue
            print(f"Shots on goal: {shotsongoal}")
        if stattype == 'Shots off Goal':
            shotsoffgoal = statvalue
            print(f"Shots off goal: {shotsoffgoal}")
        if stattype == 'Total Shots':
            totalshots = statvalue
            print(f"Total shots: {totalshots}")
        if stattype == 'Blocked Shots':
            blockedshots = statvalue
            print(f"Blocked shots: {blockedshots}")
        if stattype == 'Shots insidebox':
            shotsinsidebox = statvalue
            print(f"Shots inside box: {shotsinsidebox}")
        if stattype == 'Shots outsidebox':
            shotsoutsidebox = statvalue
            print(f"Shots outside box: {shotsoutsidebox}")
        if stattype == 'Fouls':
            fouls = statvalue
            print(f"Fouls: {fouls}")
        if stattype == 'Corner Kicks':
            cornerkicks = statvalue
            print(f"Corner kicks: {cornerkicks}")
        if stattype == 'Offsides':
            offsides = statvalue
            print(f"Offsides: {offsides}")
        if stattype == 'Ball Possession':
            ballpossessionstr = statvalue
            print(f"Ball possession string: {ballpossessionstr}")
            # Converting ballpossession to a float
            print("Converting ballpossession to a float...")
            ballpossession = percentstringtofloat(ballpossessionstr)
            print(f"Ball possession percent: {ballpossession}")
        if stattype == 'Yellow Cards':
            yellowcards = statvalue
            print(f"Yellow cards: {yellowcards}")
        if stattype == 'Red Cards':
            redcards = statvalue
            print(f"Red cards: {redcards}")
        if stattype == 'Goalkeeper Saves':
            goalkeepersaves = statvalue
            print(f"Goalkeeper saves: {goalkeepersaves}")
        if stattype == 'Total passes':
            totalpasses = statvalue
            print(f"Total passes: {totalpasses}")
        if stattype == 'Passes accurate':
            passesaccurate = statvalue
            print(f"Passes accurate: {passesaccurate}")

    ## Load into database
    sql = """
    INSERT INTO public.fixturestatistics (dbfixtureid, \
        dbteamid, \
        shotsongoal, \
        shotsoffgoal, \
        totalshots, \
        blockedshots, \
        goalkeepersaves, \
        shotsinsidebox, \
        shotsoutsidebox, \
        cornerkicks, \
        offsides, \
        ballpossession, \
        totalpasses, \
        passesaccurate, \
        fouls, \
        yellowcards, \
        redcards)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id
    """
    params = (
        databasefixtureid,
        databaseteamid,
        shotsongoal,
        shotsoffgoal,
        totalshots,
        blockedshots,
        goalkeepersaves,
        shotsinsidebox,
        shotsoutsidebox,
        cornerkicks,
        offsides,
        ballpossession,
        totalpasses,
        passesaccurate,
        fouls,
        yellowcards,
        redcards,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"New fixturestatistcs id: {newid}")

    print("")
    print("---------------------------------")




