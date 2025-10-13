import json
import psycopg2

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


# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loaddbconfig("dbconfig.json")
print("...DB config loaded.")
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

## Select eventtype = subst, teamname, player, assist from public.fixtureevent
# Select eventtype 6, 7, 9, or 12 from public.fixtureevent(eventtype); (team); (player); (assist)
# Get them into a list? dict? tuple?
with conn.cursor() as cur:
    cur.execute("select fixtureid, eventtype, team, player, assist from public.fixtureevent where eventtype in (6,7,9,12)")
    fixtureeventrows = cur.fetchall()
    print(f"Fixture event rows: {fixtureeventrows}")

for fixtureeventrow in fixtureeventrows:
    print(f"Fixture event row: {fixtureeventrow}")
    fefixtureid = fixtureeventrow[0]
    feeventtype = fixtureeventrow[1]
    feteam = fixtureeventrow[2]
    feplayer = fixtureeventrow[3]
    feassist = fixtureeventrow[4]
    print(f"FE Fixture id: {fefixtureid}")
    print(f"FE Event type: {feeventtype}")
    print(f"FE Team: {feteam}")
    print(f"FE Player: {feplayer}")
    # Get the corresponding row in the player statistics table
    with conn.cursor() as cur:
        cur.execute("select id, dbfixtureid, dbteamid, dbplayerid, substitute from public.fixtureplayerstatistics where dbfixtureid = %s and dbplayerid = %s", (fefixtureid, feteam))
        fixtureplayerstatisticsrow = cur.fetchone()
        print(f"Fixture player statistics row: {fixtureplayerstatisticsrow}")
        fpsid = fixtureplayerstatisticsrow[0]
        fpsdbfixtureid = fixtureplayerstatisticsrow[1]
        fpsdbteamid = fixtureplayerstatisticsrow[2]
        fpsdbplayerid = fixtureplayerstatisticsrow[3]
        fpssubstitute = fixtureplayerstatisticsrow[4]
        print(f"Fixture player statistics id: {fpsid}")
        print(f"DB fixture id: {fpsdbfixtureid}")
        print(f"DB team id: {fpsdbteamid}")
        print(f"DB player id: {fpsdbplayerid}")
        print(f"Substitute: {fpssubstitute}")