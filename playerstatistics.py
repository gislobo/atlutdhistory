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


def getkeyfromvalue(d, val):
    for key, value in d.items():
        if val == value:
            return key

## Initializing
# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadheaders("headers.json")
print("...headers loaded.")
print("")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loaddbconfig("dbConfig.json")
print("...DB config loaded.")
print("")

## Get api fixture id, store it as a variable
print("Getting the api fixture id...")
#apifixtureid = int(input("Enter the fixture ID:  "))
#apifixtureid = 147926
######fixtureId = 147915
apifixtureid = 147936
print(f"...fixture id is {apifixtureid}.")
# Store path to fixture info in a variable, to be used w/ connection information
print("Storing the path to the api in a path variable...")
path = f"/fixtures/players?fixture={apifixtureid}"
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
dbfixtureid = None
if apifixtureid in existingfixturesdict:
    dbfixtureid = existingfixturesdict[apifixtureid]
    print(f"The database fixture id is {dbfixtureid}.")
    print("")

## See if the fixture already has player statistics
print("Checking if the fixture already has player statistics in the database...")
with conn.cursor() as cur:
    cur.execute("select dbfixtureid from public.fixtureplayerstatistics")
    existingfixturestatisticsidsfetchall = cur.fetchall()
existingfixturestatisticsids = {row[0] for row in existingfixturestatisticsidsfetchall}
if dbfixtureid in existingfixturestatisticsids:
    print(f"The fixture {dbfixtureid} already has statistics in the database, exiting.")
    sys.exit(0)
print("...fixture does not have statistics in the database, proceeding.")
print("")

## Work out how to grab each team's statistics individually
# API tells us how many events there are
print("Getting the number of results from the api...")
apiresults = payload.get("results") or {}
print(f"The API tells us there are {apiresults} results.")
print("")

# Get the events into a list of dictionaries
print("Getting the responses into a list of dictionaries...")
response = payload.get("response") or {}
print(f"Response:  {response}.")
print(f"There are {len(response)} events in the response.")
if len(response) == apiresults:
    print("The number of events in the response matches the number of events the API initially tells us there are.  Proceeding.")
    print("")
else:
    print("Something is wrong, the number of events in the response doesn't match the number of events the API tells us there are.")
    sys.exit(0)

print("---------------------------")
print("")

## Start a for loop to grab info per player and store as variables
print("Starting the for loop to grab info per player...")
print("")
count = 0
for event in response:
    count += 1
    print(f"Loop {count}:")
    print(f"This loop's information:  {event}.")

    ## Get the database team id
    print("Getting the database team id...")
    teaminfo = event.get("team") or {}
    apiteamid = teaminfo.get("id")
    print(f"The api team id is {apiteamid}.")
    print("")

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

    ## Get the players into a list of dictionaries
    # API tells us how many players there are
    print("Getting the number of players from the api...")
    playersresults = event.get("players") or {}
    print(f"The API tells us there are {len(playersresults)} players.")
    print("")

    ## Loop through each player for individual stats
    print("Looping through each player for individual stats...")
    count2 = 0
    for player in playersresults:
        count2 += 1
        print(f"Player {count2} of {len(playersresults)}.")
        print(f"This player's information:  {player}.")

        ## Get db player id
        print("Getting the database player id...")
        playerinfo = player.get("player") or {}
        apiplayerid = playerinfo.get("id")
        print(f"This player's api player id is {apiplayerid}.")

        # Get a list of existing db player ids
        print("Getting a list of existing db player ids...")
        with conn.cursor() as cur:
            cur.execute("SELECT apifootballid, id from public.player where apifootballid = %s", (apiplayerid,))
            existingplayers = cur.fetchall()
        existingplayersdict = {existingplayer[0]: existingplayer[1] for existingplayer in existingplayers if existingplayer[0] is not None}
        dbplayerid = None
        if apiplayerid in existingplayersdict:
            dbplayerid = existingplayersdict[apiplayerid]
            print(f"The database player id is {dbplayerid}.")
            print("")
        else:
            print(f"API Player ID {apiplayerid} is not in your database.")
            sys.exit(0)

        ## Initialize statistics variables
        print("Initializing statistics variables...")
        minutes = None
        number = None
        positionid = None
        rating = None
        captain = None
        substitute = None
        offsides = None
        totalshots = None
        shotsongoal = None
        goals = None
        goalsconceded = None
        assists = None
        saves = None
        totalpasses = None
        keypasses = None
        passesaccuracy = None
        tackles = None
        blocks = None
        interceptions = None
        duels = None
        duelswon = None
        dribblesattempts = None
        dribblessuccess = None
        dribblespast = None
        foulscommitted = None
        foulsdrawn = None
        yellowcards = None
        redcards = None
        penaltieswon = None
        penaltiescommitted = None
        penaltiesscored = None
        penaltiesmissed = None
        penaltiessaved = None
        print("...variables initialized.")
        print("")

        ## Loop through the player's statistics to get them into variables
        print("Looping through the player's statistics to get them into the variables...")
        statisticslist = player.get("statistics") or []

        # Convert list to dictionary by taking the first element
        if statisticslist and isinstance(statisticslist, list) and len(statisticslist) > 0:
            statistics = statisticslist[0]
        else:
            statistics = {}
        print(f"This player's stats:  {statistics}.")
        print("")

        # Get the minutes played
        print("Getting the minutes played...")
        games = statistics.get("games") or {}
        minutes = games.get("minutes")
        print(f"This player's minutes:  {minutes}.")

        # Get the player's number worn:
        print("Getting the player's number worn...")
        number = games.get("number")
        print(f"This player's number:  {number}.")

        # Get the player's position information, turn it into db position id
        print("Getting the player's position information, and turning it into a db position id...")
        apiposition = games.get("position")
        print(f"Api position:  {apiposition}.")
        # Query the database for position id, if it's not there, insert it
        with conn.cursor() as cur:
            cur.execute("select position, id from public.position")
            existingpositions = cur.fetchall()
        existingpositionsdict = {existingposition[0]: existingposition[1] for existingposition in existingpositions if existingposition[0] is not None}
        if apiposition in existingpositionsdict:
            positionid = existingpositionsdict[apiposition]
            print(f"The position already exists in the database, position id:  {positionid}.")
        else:
            print(f"The position does not exist in the database, inserting it...")
            with conn:
                with conn.cursor() as cur:
                    cur.execute("insert into public.position (position) values (%s) returning id", (apiposition,))
                    positionid = cur.fetchone()[0]
                    print(f"...position inserted, position id:  {positionid}.")

        # Get the player's rating, turn it from a string to numeric
        print("Getting the player's rating, and turning it from a string to numeric...")
        ratingstr = games.get("rating")
        if ratingstr is not None:
            rating = float(ratingstr)
        print(f"This player's rating:  {rating}.")

        # Get the player's captain status, it's a boolean
        print("Getting the player's captain status, it's a boolean...")
        captain = games.get("captain")
        print(f"This player's captain status:  {captain}.")

        # Get the player's substitute status, it's a boolean
        print("Getting the player's substitute status, it's a boolean...")
        substitute = games.get("substitute")
        print(f"This player's substitute status:  {substitute}.")

        # Get how many times the player was flagged for offsides
        print("Getting how many times the player was flagged for offsides...")
        offsides = statistics.get("offsides")
        print(f"This player's offsides:  {offsides}.")

        # Get the player's shots information
        print("Getting the player's shots information...")
        shots = statistics.get("shots") or {}
        totalshots = shots.get("total")
        shotsongoal = shots.get("on")
        print(f"This player's total shots:  {totalshots}.")
        print(f"This player's shots on goal:  {shotsongoal}.")

        # Get the player's goals information
        print("Getting the player's goals information...")
        goalsinfo = statistics.get("goals") or {}
        goals = goalsinfo.get("total")
        goalsconceded = goalsinfo.get("conceded")
        assists = goalsinfo.get("assists")
        saves = goalsinfo.get("saves")
        print(f"This player's goals:  {goals}.")
        print(f"This player's goals conceded:  {goalsconceded}.")
        print(f"This player's assists:  {assists}.")
        print(f"This player's saves:  {saves}.")

        # Get the player's passes information
        print("Getting the player's passes information...")
        passes = statistics.get("passes") or {}
        totalpasses = passes.get("total")
        keypasses = passes.get("key")
        passesaccuracystr = passes.get("accuracy")
        print(f"This player's total passes:  {totalpasses}.")
        print(f"This player's key passes:  {keypasses}.")
        if passesaccuracystr is not None:
            passesaccuracy = float(passesaccuracystr.strip('%'))
        print(f"This player's passes accuracy:  {passesaccuracy}.")

        # Get the player's tackles information
        print("Getting the player's tackles information...")
        tacklesinfo = statistics.get("tackles") or {}
        tackles = tacklesinfo.get("total")
        blocks = tacklesinfo.get("blocks")
        interceptions = tacklesinfo.get("interceptions")
        print(f"This player's tackles:  {tackles}.")
        print(f"This player's blocks:  {blocks}.")
        print(f"This player's interceptions:  {interceptions}.")

        # Get the player's duels information
        print("Getting the player's duels information...")
        duelsinfo = statistics.get("duels") or {}
        duels = duelsinfo.get("total")
        duelswon = duelsinfo.get("won")
        print(f"This player's duels:  {duels}.")
        print(f"This player's duels won:  {duelswon}.")

        # Get the player's dribbles information
        print("Getting the player's dribbles information...")
        dribblesinfo = statistics.get("dribbles") or {}
        dribblesattempts = dribblesinfo.get("attempts")
        dribblessuccess = dribblesinfo.get("success")
        dribblespast = dribblesinfo.get("past")
        print(f"This player's dribbles attempts:  {dribblesattempts}.")
        print(f"This player's dribbles success:  {dribblessuccess}.")
        print(f"This player's dribbles past:  {dribblespast}.")

        # Get the player's fouls information
        print("Getting the player's fouls information...")
        foulsinfo = statistics.get("fouls") or {}
        foulscommitted = foulsinfo.get("committed")
        foulsdrawn = foulsinfo.get("drawn")
        print(f"This player's fouls committed:  {foulscommitted}.")
        print(f"This player's fouls drawn:  {foulsdrawn}.")

        # Get the player's cards information
        print("Getting the player's cards information...")
        cardsinfo = statistics.get("cards") or {}
        yellowcards = cardsinfo.get("yellow")
        redcards = cardsinfo.get("red")
        print(f"This player's yellow cards:  {yellowcards}.")
        print(f"This player's red cards:  {redcards}.")

        # Get the player's penalties information
        print("Getting the player's penalties information...")
        penaltiesinfo = statistics.get("penalty") or {}
        penaltieswon = penaltiesinfo.get("won")
        penaltiescommitted = penaltiesinfo.get("committed")
        penaltiesscored = penaltiesinfo.get("scored")
        penaltiesmissed = penaltiesinfo.get("missed")
        penaltiessaved = penaltiesinfo.get("saved")
        print(f"This player's penalties won:  {penaltieswon}.")
        print(f"This player's penalties committed:  {penaltiescommitted}.")
        print(f"This player's penalties scored:  {penaltiesscored}.")
        print(f"This player's penalties missed:  {penaltiesmissed}.")
        print(f"This player's penalties saved:  {penaltiessaved}.")
        print("")
        print("We now have all of this player's stats into variables.")
        print("")

        # Now we insert this information into the database
        print("Now we insert this information into the database...")
        sql = """
        insert into public.fixtureplayerstatistics (
            dbfixtureid, \
            dbteamid, \
            dbplayerid, \
            minutes, \
            number, \
            positionid, \
            rating, \
            captain, \
            substitute, \
            offsides, \
            totalshots, \
            shotsongoal, \
            goals, \
            goalsconceded, \
            assists, \
            saves, \
            totalpasses, \
            keypasses, \
            passesaccuracy, \
            tackles, \
            blocks, \
            interceptions, \
            duels, \
            duelswon, \
            dribblesattempts, \
            dribblessuccess, \
            dribblespast, \
            foulscommitted, \
            foulsdrawn, \
            yellowcards, \
            redcards, \
            penaltieswon, \
            penaltiescommitted, \
            penaltiesscored, \
            penaltiesmissed, \
            penaltiessaved)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id
        """
        params = (
            dbfixtureid,
            dbteamid,
            dbplayerid,
            minutes,
            number,
            positionid,
            rating,
            captain,
            substitute,
            offsides,
            totalshots,
            shotsongoal,
            goals,
            goalsconceded,
            assists,
            saves,
            totalpasses,
            keypasses,
            passesaccuracy,
            tackles,
            blocks,
            interceptions,
            duels,
            duelswon,
            dribblesattempts,
            dribblessuccess,
            dribblespast,
            foulscommitted,
            foulsdrawn,
            yellowcards,
            redcards,
            penaltieswon,
            penaltiescommitted,
            penaltiesscored,
            penaltiesmissed,
            penaltiessaved,
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                newid = cur.fetchone()[0]
                print(f"...insert successful, new id:  {newid}.")

        print("")
        print("---------------------------------")
        print("")