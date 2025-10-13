import http.client
import json
import psycopg2
import unicodedata
    
    
def loadheaders(headerspath="headers.json"):
    with open(headerspath, "r", encoding="utf-8") as f:
        return json.load(f)


def loaddbconfig(configpath="dbConfig.json"):
    with open(configpath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return{
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


def getplayers(payload, playerids):
    # apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
    # fixtureid = f
    # #fixtureid = int(input("Enter the Fixture ID:  "))
    # #fixtureid = 147926
    # #fixtureid = 147915
    # #fixtureid = 147936
    # path = f"/fixtures?id={fixtureid}"
    #
    # conn.request("GET", path, headers=headers)
    # res = conn.getresponse()
    # raw = res.read()
    #
    # payload = json.loads(raw.decode("utf-8"))
    # print(payload)

    for item in payload.get("response", []):
        lineups = item.get("lineups") or []
        if not isinstance(lineups, list):
            continue
        for lineup in lineups:
            # Extract starters
            for s in (lineup.get("startXI") or []):
                player = (s or {}).get("player") or {}
                pid = player.get("id")
                if pid and pid not in playerids:
                    playerids.append(pid)
            # Extract substitutes
            for s in (lineup.get("substitutes") or []):
                player = (s or {}).get("player") or {}
                pid = player.get("id")
                if pid and pid not in playerids:
                    playerids.append(pid)



def getplayerprofile(headers, playerid):
    playerconn = http.client.HTTPSConnection("v3.football.api-sports.io")
    playerpath = f"/players/profiles?player={playerid}"
    normalized = {}

    playerconn.request("GET", playerpath, headers=headers)
    playerres = playerconn.getresponse()
    playerraw = playerres.read()

    playerpayload = json.loads(playerraw.decode("utf-8"))

    for item in playerpayload.get("response", []):
        p = item.get("player") or {}
        birth = p.get("birth") or {}
        pid = p.get("id")
        if pid is None:
            # Skip players without an ID
            continue

        normalized[pid] = {
            "apifootballid": pid,
            "firstname": p.get("firstname"),
            "lastname": p.get("lastname"),
            "birthdate": birth.get("date"),
            "birthplace": birth.get("place"),
            "birthcountrycode": birth.get("country"),
            "nationality": p.get("nationality"),
            "heightcm": parseheightweight(p.get("height")),
            "weightkg": parseheightweight(p.get("weight")),
        }
    playerconn.close()
    return normalized


def parseheightweight(str):
    if not str:
        return None
    try:
        return int(str.split()[0])
    except Exception:
        return None


def applycountrycodes(conn, country):
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

    with conn.cursor() as cur:
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


def normalizename(s: str | None) -> str | None:
    if s and s.strip():
        return s.strip().lower()
    return None


def getpositionid(conn, positionname):
    print(f"Looking up {positionname}...")

    with conn.cursor() as cur:
        cur.execute("SELECT position FROM public.position")
        rows = cur.fetchall()
    existingpositions = {row[0] for row in rows if row[0] is not None}
    if positionname in existingpositions:
        print(f"Position {positionname} is already in the database.")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM public.position
                WHERE position = %s
                """,
                (positionname,)
            )
            positionId = cur.fetchone()[0]
            return positionId
    else:
        print(f"Position {positionname} is not in the database.")
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.position (position) VALUES (%s) RETURNING id",
                (positionname,),
            )
            positionId = cur.fetchone()[0]
            return positionId


def playerlookup(headers, conn, playerid):
    with conn.cursor() as cur:
        cur.execute("SELECT apifootballid FROM public.player")
        rows = cur.fetchall()
    existingplayers = {row[0] for row in rows if row[0] is not None}

    if playerid in existingplayers:
        print(f"Player {playerid} is already in the database, no need to proceed.")
        return
    else:
        print(f"Player {playerid} is not in the database, proceeding.")
        builddictionary(headers, conn, playerid)


def builddictionary(headers, conn, playerid):
    print(f"Building the dictionary for {playerid}...")
    player = getplayerprofile(headers, playerid)
    print("...dictionary built.")
    print("Replacing birthcountry and nationality with codes from database...")
    birthcountryname = player.get(playerid).get("birthcountrycode")
    nationalityname = player.get(playerid).get("nationality")
    print(f"Birth country name:  {birthcountryname}.")
    print(f"Nationality name:  {nationalityname}.")

    with conn:
        # Map birthcountry name to code in database and replace dict value
        print("Map birthcountry name to code in database and replace dict value...")
        birthcountrycodemap = applycountrycodes(conn, birthcountryname)
        if birthcountrycodemap:
            birthcountrycode = next(iter(birthcountrycodemap.values()))
        else:
            birthcountrycode = None  # keep NULL if not found
            print(f"Warning: No match found for birth country '{birthcountryname}'. Leaving NULL.")
        player[playerid]["birthcountrycode"] = birthcountrycode
        print("...done.")
        # Map nationality name to code in database and replace dict value
        print("Map nationality name to code in database and replace dict value...")
        nationalitycodemap = applycountrycodes(conn, nationalityname)
        if nationalitycodemap:
            nationalitycountrycode = next(iter(nationalitycodemap.values()))
        else:
            nationalitycountrycode = None  # keep NULL if not found
            print(f"Warning: No match found for nationality '{nationalityname}'. Leaving NULL.")
        player[playerid]["nationality"] = nationalitycountrycode
        print("...done.")

        # positionId = getpositionid(conn, positionname)
        # print(positionId)
        # player[playerid]["position"] = positionId
        print(player)

    sql = """
        INSERT INTO public.player (apifootballid, \
                                   firstname, \
                                   lastname, \
                                   birthdate, \
                                   birthplace, \
                                   birthcountrycode, \
                                   nationality, \
                                   heightcm, \
                                   weightkg) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    params = (
        playerid,
        player[playerid]["firstname"],
        player[playerid]["lastname"],
        player[playerid]["birthdate"],
        player[playerid]["birthplace"],
        player[playerid]["birthcountrycode"],
        player[playerid]["nationality"],
        player[playerid]["heightcm"],
        player[playerid]["weightkg"],
       # player[playerid]["position"],
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"Player {playerid} inserted with id {newid}.")


def players(payload, f, headers, conn):
    playerids = []
    print(f"Getting player IDs...")
    getplayers(payload, playerids)
    print(f"Player IDs:  {playerids}.")

    for playerid in playerids:
        playerlookup(headers, conn, playerid)


def main():
    # list out fixtures
    fixturelist = []
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

    # Connect once for lookups and load
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
    )

    for fixture in fixturelist:
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        # fixtureid = int(input("Enter the Fixture ID:  "))
        # fixtureid = 147926
        # fixtureid = 147915
        # fixtureid = 147936
        path = f"/fixtures?id={fixture}"

        apiconn.request("GET", path, headers=headers)
        res = apiconn.getresponse()
        raw = res.read()
        payload = json.loads(raw.decode("utf-8"))
        print(payload)
        players(payload, fixture, headers, conn)



if __name__ == "__main__":
    main()