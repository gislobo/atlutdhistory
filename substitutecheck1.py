import json
import psycopg2


def loaddbconfig(configPath="dbConfig.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loaddbconfig("dbConfig.json")
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
    cur.execute(
        "select id, fixtureid, eventtype, team, assist from public.fixtureevent where eventtype in (6,7,9,12)")
    fixtureeventrows = cur.fetchall()
    print(f"Fixture event rows: {fixtureeventrows}")

for fixtureeventrow in fixtureeventrows:
    print(f"Fixture event row: {fixtureeventrow}")
    feid = fixtureeventrow[0]
    fefixtureid = fixtureeventrow[1]
    feeventtype = fixtureeventrow[2]
    feteam = fixtureeventrow[3]
    feassist = fixtureeventrow[4]
    print(f"Fixtureevent id: {feid}")
    print(f"Fixtureevent fixtureid: {fefixtureid}")
    print(f"Fixtureevent eventtype: {feeventtype}")
    print(f"Fixtureevent team: {feteam}")
    print(f"Fixtureevent assist: {feassist}")

    with conn.cursor() as cur:
        cur.execute(
            """
            select id, dbfixtureid, dbteamid, dbplayerid, substitute
            from public.fixtureplayerstatistics
            where dbfixtureid = %s and dbteamid = %s and dbplayerid = %s
            """,
            (fefixtureid, feteam, feassist,)
        )
        fpsrows = cur.fetchall()
        print(f"Fixture player statistics rows: {fpsrows}")
        try:
            print(type(fpsrows[0]))
            for fpsrow in fpsrows:
                print(fpsrow)
                fpsid = fpsrow[0]
                fpsdbfixtureid = fpsrow[1]
                fpsdbteamid = fpsrow[2]
                fpsdbplayerid = fpsrow[3]
                fpssubstitute = fpsrow[4]
                print(f"FPS id: {fpsid}")
                print(f"FPS fixture id: {fpsdbfixtureid}")
                print(f"FPS team id: {fpsdbteamid}")
                print(f"FPS player id: {fpsdbplayerid}")
                print(f"Substitute: {fpssubstitute}")
                if not fpssubstitute:
                    print("Substitute is not true.")
                    sql = """
                    update public.fixtureplayerstatistics
                    set substitute = true
                    where id = %s
                    """
                    params = (fpsid,)
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(sql, params)
                            print("Update executed.")
        except IndexError:
            print("Index error, no player statistics found.")


        print("")

conn.close()
print("Connection closed.")