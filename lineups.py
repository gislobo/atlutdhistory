import json
import http.client
import sys
import psycopg2
import unicodedata

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


def applycountrycodes(c1, country):
    def country_lookup_candidates(name):
        if not name:
            return []
        s = str(name).strip()
        candidates = set()

        def add(v):
            if v and v.strip():
                candidates.add(" ".join(v.strip().lower().split()))

        # Base
        add(s)
        # Hyphen/space variants
        add(s.replace("-", " "))
        add(s.replace(" ", "-"))
        # Remove punctuation except hyphens
        s_no_punct = "".join(ch for ch in s if ch.isalnum() or ch.isspace() or ch == "-")
        add(s_no_punct)
        add(s_no_punct.replace("-", " "))
        add(s_no_punct.replace(" ", "-"))
        # Accent fold
        s_ascii = unicodedata.normalize("NFKD", s)
        s_ascii = "".join(ch for ch in s_ascii if not unicodedata.combining(ch))
        add(s_ascii)
        add(s_ascii.replace("-", " "))
        add(s_ascii.replace(" ", "-"))

        # Special-case: Republic of Ireland -> also match Ireland
        s_lower_spaces = " ".join(s.strip().lower().replace("-", " ").split())
        if "republic of ireland" in s_lower_spaces:
            add("ireland")

        return sorted(candidates)

    candidates = country_lookup_candidates(country)
    print(f"Looking up candidates: {candidates!r}")

    if not candidates:
        return {}

    with c1.cursor() as cur:
        cur.execute(
            """
            SELECT LOWER(name) AS lname, code
            FROM public.country
            WHERE LOWER(name) = ANY(%s)
            """,
            (candidates,)
        )
        rows = cur.fetchall()
    return {lname: code for lname, code in rows}


def coachwork(ac, h, aid, c):
    ## Call the coach info from api
    # Set the api path
    coachespath = f"/coachs?id={aid}"
    print("Making the request to the api...")
    ac.request("GET", coachespath, headers=h)
    cres = ac.getresponse()
    craw = cres.read()
    cpayload = json.loads(craw.decode("utf-8"))
    print(f"coach payload: {cpayload}")

    ## Get coach info into variables
    # Initialize variables
    firstname = None
    lastname = None
    birthdate = None
    birthplace = None
    birthcountry = None
    birthcountrycodemap = None
    birthcountrycode = None
    apinationality = None
    nationalitycodemap = None
    nationality = None

    # Get response from payload
    cresponselist = cpayload.get("response") or []

    # Convert list to dictionary by taking the first element
    if cresponselist and isinstance(cresponselist, list) and len(cresponselist) > 0:
        cresponse = cresponselist[0]
    else:
        cresponse = {}
    print(f"coach response:  {cresponse}.")

    # Get the names
    firstname = cresponse.get("firstname")
    lastname = cresponse.get("lastname")
    print(f"coach firstname:  {firstname}.")
    print(f"coach lastname:  {lastname}.")

    # Get birth info
    birthinfo = cresponse.get("birth") or {}
    birthdate = birthinfo.get("date")
    birthplace = birthinfo.get("place")
    birthcountry = birthinfo.get("country")
    birthcountrycodemap = applycountrycodes(c, birthcountry)
    if birthcountrycodemap:
        birthcountrycode = next(iter(birthcountrycodemap.values()))
    else:
        birthcountrycode = None
        print(f"Warning: No match found for birth country '{birthcountry}'. Leaving NULL.")
    print(f"coach birthdate:  {birthdate}.")
    print(f"coach birthplace:  {birthplace}.")
    print(f"coach birthcountry:  {birthcountry}.")
    print(f"coach birthcountrycode:  {birthcountrycode}.")

    # Get nationality info
    apinationality = cresponse.get("nationality")
    nationalitycodemap = applycountrycodes(c, apinationality)
    if nationalitycodemap:
        nationality = next(iter(nationalitycodemap.values()))
    else:
        nationality = None
        print(f"Warning: No match found for nationality '{apinationality}'. Leaving NULL.")
    print(f"coach apinationality:  {apinationality}.")
    print(f"coach nationality:  {nationality}.")

    ## Load into database
    sql = """
    insert into public.coach (apifootballid, firstname, lastname, birthdate, birthplace, birthcountrycode, nationality)
        values (%s, %s, %s, %s, %s, %s, %s) returning id
    """
    params = (
        aid,
        firstname,
        lastname,
        birthdate,
        birthplace,
        birthcountrycode,
        nationality
    )

    with c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"New coach id: {newid}.")

    return newid


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

## Get api fixture id, store it as a variable
print("Getting the api fixture id...")
apifixtureid = int(input("Enter the fixture ID:  "))
#apifixtureid = 147926
######fixtureId = 147915
#apifixtureid = 147936
print(f"...fixture id is {apifixtureid}.")
# Store path to fixture info in a variable, to be used w/ connection information
print("Storing the path to the api in a path variable...")
path = f"/fixtures/lineups?fixture={apifixtureid}"
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
fixtureid = None
if apifixtureid in existingfixturesdict:
    fixtureid = existingfixturesdict[apifixtureid]
    print(f"The database fixture id is {fixtureid}.")
    print("")

## See if the fixture already has lineups
print("Checking if the fixture already has lineups in the database...")
with conn.cursor() as cur:
    cur.execute("select fixtureid from public.fixturelineups")
    existingfixturelineupsidsfetchall = cur.fetchall()
existingfixturelineupsids = {row[0] for row in existingfixturelineupsidsfetchall}
if fixtureid in existingfixturelineupsids:
    print(f"The fixture {fixtureid} already has lineups in the database, exiting.")
    sys.exit(0)
print("...fixture does not have lineups in the database, proceeding.")
print("")

# Get the response into a list of dictionaries
print("Getting the response into a list of dictionaries...")
response = payload.get("response") or {}
print(response)
if len(response) != 2:
    print("Something is wrong, the number of responses is not 2.")
    sys.exit(0)
else:
    print(f"There are {len(response)} responses, and there should be 2.")
    print("")

print("---------------------------")
print("")

## Starting a loop to grab info and store the variables
print("Starting the loop to grab info and store the variables...")
print("")
count = 0
for event in response:
    count += 1
    print(f"Loop {count}:")
    print(f"This loop's information:  {event}.")
    print("")

    ## Get the database team id
    print("Getting the database team id...")
    teaminfo = event.get("team") or {}
    apiteamid = teaminfo.get("id")
    print(f"The api team id is {apiteamid}.")

    # Get a list of existing db team ids
    print("Getting a list of existing db team ids...")
    with conn.cursor() as cur:
        cur.execute("SELECT apifootballid, id from public.team where apifootballid = %s", (apiteamid,))
        existingteams = cur.fetchall()
    existingteamsdict = {existingteam[0]: existingteam[1] for existingteam in existingteams if existingteam[0] is not None}
    dbteamid = None
    if apiteamid in existingteamsdict:
        dbteamid = existingteamsdict[apiteamid]
        print(f"The database team id is {dbteamid}.")
        print("")
    else:
        print(f"API Team ID {apiteamid} is not in your database.")
        sys.exit(0)

    ## Get the coach id
    print("Getting the coach id...")
    coachinfo = event.get("coach") or {}
    apicoachid = coachinfo.get("id")
    print(f"The api coach id is {apicoachid}.")

    # Get a list of existing db coach ids
    print("Getting a list of existing db coach ids...")
    with conn.cursor() as cur:
        cur.execute("select apifootballid, id from public.coach where apifootballid = %s", (apicoachid,))
        existingcoaches = cur.fetchall()
    existingcoachesdict = {existingcoach[0]:  existingcoach[1] for existingcoach in existingcoaches if existingcoach[0] is not None}
    coachid = None
    if apicoachid in existingcoachesdict:
        coachid = existingcoachesdict[apicoachid]
        print(f"The database coach id is {coachid}.")
        print("")
    else:
        print(f"API Coach ID {apicoachid} is not in your database.")
        print("Adding coach to database...")
        coachid = coachwork(apiconn, headers, apicoachid, conn)
        print(f"The coach id is {coachid}.")
        print("")

    ## Get the formation id
    formation = event.get("formation")
    print(f"The formation is {formation}.")

    # Get a list of existing db formations
    print("Getting a list of existing db formations...")
    with conn.cursor() as cur:
        cur.execute("select formation, id from public.formation")
        existingformations = cur.fetchall()
    existingformationsdict = {existingformation[0]: existingformation[1] for existingformation in existingformations if existingformation[0] is not None}
    formationid = None
    if formation in existingformationsdict:
        formationid = existingformationsdict[formation]
        print(f"The database formation id is {formationid}.")
        print("")
    else:
        print(f"Formation {formation} is not in your database.")
        print("Adding formation to database...")
        with conn:
            with conn.cursor() as cur:
                cur.execute("insert into public.formation (formation) values (%s) returning id", (formation,))
                formationid = cur.fetchone()[0]
                print(f"The formation id is {formationid}.")
                print("")

    ## Get the player ids
    # Get the starter ids
    print("Getting the starter ids...")
    starters = event.get("startXI") or []

    # Initialize starter variables
    starter1 = None
    starter2 = None
    starter3 = None
    starter4 = None
    starter5 = None
    starter6 = None
    starter7 = None
    starter8 = None
    starter9 = None
    starter10 = None
    starter11 = None

    # Loop through starters
    print("Looping through starters...")
    count1 = 0
    for starter in starters:
        count1 += 1
        print(f"This starter is {starter}.")
        player = starter.get("player") or {}
        apiplayerid = player.get("id")
        print(f"The api player id is {apiplayerid}.")

        # Get db player id
        with conn.cursor() as cur:
            cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
            databaseplayerid = cur.fetchone()[0]
        print(f"The database player id is {databaseplayerid}.")

        # Set starter variables
        if count1 == 1:
            starter1 = databaseplayerid
            print(f"Starter 1 is {starter1}.")
        elif count1 == 2:
            starter2 = databaseplayerid
            print(f"Starter 2 is {starter2}.")
        elif count1 == 3:
            starter3 = databaseplayerid
            print(f"Starter 3 is {starter3}.")
        elif count1 == 4:
            starter4 = databaseplayerid
            print(f"Starter 4 is {starter4}.")
        elif count1 == 5:
            starter5 = databaseplayerid
            print(f"Starter 5 is {starter5}.")
        elif count1 == 6:
            starter6 = databaseplayerid
            print(f"Starter 6 is {starter6}.")
        elif count1 == 7:
            starter7 = databaseplayerid
            print(f"Starter 7 is {starter7}.")
        elif count1 == 8:
            starter8 = databaseplayerid
            print(f"Starter 8 is {starter8}.")
        elif count1 == 9:
            starter9 = databaseplayerid
            print(f"Starter 9 is {starter9}.")
        elif count1 == 10:
            starter10 = databaseplayerid
            print(f"Starter 10 is {starter10}.")
        elif count1 == 11:
            starter11 = databaseplayerid
            print(f"Starter 11 is {starter11}.")
        else:
            print("Something is wrong, the number of starters is not 11.")
            sys.exit(0)

    # Get the substitute ids
    print("Getting the substitute ids...")
    substitutes = event.get("substitutes") or []

    # Initialize substitute variables
    substitute1 = None
    substitute2 = None
    substitute3 = None
    substitute4 = None
    substitute5 = None
    substitute6 = None
    substitute7 = None

    # Loop through substitutes
    print("Looping through substitutes...")
    count2 = 0
    for substitute in substitutes:
        count2 += 1
        print(f"This substitute is {substitute}.")
        player = substitute.get("player") or {}
        apiplayerid = player.get("id")
        print(f"The api player id is {apiplayerid}.")

        # Get db player id
        with conn.cursor() as cur:
            cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
            databaseplayerid = cur.fetchone()[0]
        print(f"The database player id is {databaseplayerid}.")

        # Set substitute variables
        if count2 == 1:
            substitute1 = databaseplayerid
            print(f"Substitute 1 is {substitute1}.")
        elif count2 == 2:
            substitute2 = databaseplayerid
            print(f"Substitute 2 is {substitute2}.")
        elif count2 == 3:
            substitute3 = databaseplayerid
            print(f"Substitute 3 is {substitute3}.")
        elif count2 == 4:
            substitute4 = databaseplayerid
            print(f"Substitute 4 is {substitute4}.")
        elif count2 == 5:
            substitute5 = databaseplayerid
            print(f"Substitute 5 is {substitute5}.")
        elif count2 == 6:
            substitute6 = databaseplayerid
            print(f"Substitute 6 is {substitute6}.")
        elif count2 == 7:
            substitute7 = databaseplayerid
            print(f"Substitute 7 is {substitute7}.")
        else:
            print("Something is wrong, the number of substitutes is not 7.")
            sys.exit(0)

    # Inserting variables into database
    print("Inserting variables into database...")
    sql = """
    insert into public.fixturelineups (
        fixtureid, \
        teamid, \
        coachid, \
        formationid, \
        starter1, \
        starter2, \
        starter3, \
        starter4, \
        starter5, \
        starter6, \
        starter7, \
        starter8, \
        starter9, \
        starter10, \
        starter11, \
        substitute1, \
        substitute2, \
        substitute3, \
        substitute4, \
        substitute5, \
        substitute6, \
        substitute7)
    values (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id 
    """
    params = (
        fixtureid,
        dbteamid,
        coachid,
        formationid,
        starter1,
        starter2,
        starter3,
        starter4,
        starter5,
        starter6,
        starter7,
        starter8,
        starter9,
        starter10,
        starter11,
        substitute1,
        substitute2,
        substitute3,
        substitute4,
        substitute5,
        substitute6,
        substitute7,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"...insert successful, new id: {newid}.")

    print("")
    print("---------------------------------")
    print("")