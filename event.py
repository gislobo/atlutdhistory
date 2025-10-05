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


def eventtypework(c, et, ed):
    # Get the event type information into a list of dictionaries
    print("Starting the eventypework function.")
    with c.cursor() as cur:
        cur.execute("SELECT id, type, eventdetail FROM public.eventtype")
        rows = cur.fetchall()
    print(f"Rows of eventtype: {rows}")

    # Check to see if the event type exists in the database
    eventtypeexists = False
    dbeventtypeid = ""
    for row in rows:
        print(f"Row id {row[0]}, type {row[1]}, eventdetail {row[2]}")
        if row[1] == et and row[2] == ed:
            print(f"Found event type {et} and event detail {ed}.")
            dbeventtypeid = row[0]
            eventtypeexists = True
            break

    # If not in the database, add them in as a new row in public.eventtype
    if not eventtypeexists:
        print(f"Event type {et} and event detail {ed} not found in database.")
        with c:
            with c.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.eventtype (type, eventdetail) VALUES (%s, %s) RETURNING id",
                    (et, ed),
                )
                dbeventtypeid = cur.fetchone()[0]
                print(f"Event type {et} and event detail {ed} inserted with id {dbeventtypeid}.")

    print("Ending the eventypework function.")
    return dbeventtypeid


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
path = f"/fixtures/events?fixture={apifixtureid}"

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

## Check to see if the fixture has events already in the table
with conn.cursor() as cur:
    cur.execute("SELECT fixtureid from public.fixtureevent")
    existingfixtureevents = cur.fetchall()
existingids = {row[0] for row in existingfixtureevents}  # Extract first column into a set
print(f"Existing fixture events: {existingids}")
if databasefixtureid in existingids:
    print(f"The fixture {databasefixtureid} already has events in the database.")
    sys.exit(0)

## Work out how to grab each event individually
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

    ## Event type work
    # Get type, detail, and comments into variables
    eventtype = event.get("type")
    eventdetail = event.get("detail")
    eventcomments = event.get("comments")
    print(f"Event type: {eventtype}")
    print(f"Event detail: {eventdetail}")
    print(f"Event comments: {eventcomments}")

    # Write a function to get the database id for the event type
    eventypeid = eventtypework(conn, eventtype, eventdetail)
    print(f"Event type id: {eventypeid}")
    print("")

    ## Time elapsed and extratimeelapsed
    # Get time info per event into their respective variables
    timeinfo = event.get("time") or {}
    print(f"Time info: {timeinfo}")
    elapsed = timeinfo.get("elapsed")
    extra = timeinfo.get("extra")
    print(f"Elapsed time: {elapsed}")
    print(f"Extra time elapsed: {extra}")
    print("")

    ## Get database team id
    teaminfo = event.get("team") or {}
    apiteamid = teaminfo.get("id")
    print(f"apiteamid: {apiteamid}")
    databaseteamid = ""
    with conn.cursor() as cur:
        cur.execute("SELECT id from public.team WHERE apifootballid = %s", (apiteamid,))
        databaseteamid = cur.fetchone()[0]
    print(f"Database team id: {databaseteamid}")
    print("")

    ## Get database player id
    playerinfo = event.get("player") or {}
    apiplayerid = playerinfo.get("id")
    print(f"apiplayerid: {apiplayerid}")
    databaseplayerid = ""
    with conn.cursor() as cur:
        cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
        databaseplayerid = cur.fetchone()[0]
    print(f"Database player id: {databaseplayerid}")
    print("")

    ## Assist work (database player id)
    assistinfo = event.get("assist") or {}
    apiassistid = assistinfo.get("id")
    databaseassistid = ""
    if apiassistid is None:
        databaseassistid = None
        print(f"Assist id is None.")
    else:
        with conn.cursor() as cur:
            cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiassistid,))
            databaseassistid = cur.fetchone()[0]
        print(f"Assist id: {databaseassistid}")

    ## Load into database
    sql = """
    INSERT INTO public.fixtureevent (fixtureid, \
                                     eventtype, \
                                     eventcomments, \
                                     timeelapsed, \
                                     extratimeelapsed, \
                                     team, \
                                     player, \
                                     assist)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    params = (
        databasefixtureid,
        eventypeid,
        eventcomments,
        elapsed,
        extra,
        databaseteamid,
        databaseplayerid,
        databaseassistid,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            eventid = cur.fetchone()[0]
            print(f"Event inserted with id {eventid}.")

    print("")
    print("---------------------------------")