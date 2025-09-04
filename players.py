import http.client
import json
import psycopg2
import unicodedata


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


def getPlayers(headers, playerIds):
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")
    fixtureId = int(input("Enter the Fixture ID:  "))
    #fixtureId = 147926
    #fixtureId = 147915
    #fixtureId = 147936
    path = f"/fixtures?id={fixtureId}"

    conn.request("GET", path, headers=headers)
    res = conn.getresponse()
    raw = res.read()

    payload = json.loads(raw.decode("utf-8"))
    print(payload)

    for item in payload.get("response", []):
        lineups = item.get("lineups") or []
        if not isinstance(lineups, list):
            continue
        for lineup in lineups:
            # Extract starters
            for s in (lineup.get("startXI") or []):
                player = (s or {}).get("player") or {}
                pid = player.get("id")
                if pid and pid not in playerIds:
                    playerIds.append(pid)
            # Extract substitutes
            for s in (lineup.get("substitutes") or []):
                player = (s or {}).get("player") or {}
                pid = player.get("id")
                if pid and pid not in playerIds:
                    playerIds.append(pid)
    conn.close()


def getPlayerProfile(headers, playerId):
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")
    path = f"/players/profiles?player={playerId}"
    normalized = {}

    conn.request("GET", path, headers=headers)
    res = conn.getresponse()
    raw = res.read()

    payload = json.loads(raw.decode("utf-8"))

    for item in payload.get("response", []):
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
            "heightcm": parseHeightWeight(p.get("height")),
            "weightkg": parseHeightWeight(p.get("weight")),
            #"position": p.get("position")
        }
    conn.close()
    return normalized


def parseHeightWeight(str):
    if not str:
        return None
    try:
        return int(str.split()[0])
    except Exception:
        return None


def applyCountryCodes(conn, country):
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


def normalizeName(s: str | None) -> str | None:
    if s and s.strip():
        return s.strip().lower()
    return None


def getPositionId(conn, positionname):
    print(f"Looking up {positionname}...")

    with conn.cursor() as cur:
        cur.execute("SELECT position FROM public.position")
        rows = cur.fetchall()
    existingPositions = {row[0] for row in rows if row[0] is not None}
    if positionname in existingPositions:
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


def playerLookup(conn, playerId):
    with conn.cursor() as cur:
        cur.execute("SELECT apifootballid FROM public.player")
        rows = cur.fetchall()
    existingPlayers = {row[0] for row in rows if row[0] is not None}

    if playerId in existingPlayers:
        print(f"Player {playerId} is already in the database, no need to proceed.")
        return
    else:
        print(f"Player {playerId} is not in the database, proceeding.")
        buildDictionary(conn, playerId)


def buildDictionary(conn, playerId):
    print(f"Building the dicitonary for {playerId}...")
    player = getPlayerProfile(headers, playerId)
    print("...dictionary built.")
    print("Replacing birthcountry and nationality with codes from database...")
    # print(player.get(playerId).get("position"))
    birthcountryname = player.get(playerId).get("birthcountrycode")
    nationalityname = player.get(playerId).get("nationality")
    #positionname = player.get(playerId).get("position")
    print(birthcountryname)
    print(nationalityname)
    #print(positionname)

    with conn:
        # Map birthcountry name to code in database and replace dict value
        print("Map birthcountry name to code in database and replace dict value...")
        birthCountryCodeMap = applyCountryCodes(conn, birthcountryname)
        if birthCountryCodeMap:
            birthCountryCode = next(iter(birthCountryCodeMap.values()))
        else:
            birthCountryCode = None  # keep NULL if not found
            print(f"Warning: No match found for birth country '{birthcountryname}'. Leaving NULL.")
        player[playerId]["birthcountrycode"] = birthCountryCode
        print("...done.")
        # Map nationality name to code in database and replace dict value
        print("Map nationality name to code in database and replace dict value...")
        nationalityCodeMap = applyCountryCodes(conn, nationalityname)
        if nationalityCodeMap:
            nationalityCountryCode = next(iter(nationalityCodeMap.values()))
        else:
            nationalityCountryCode = None  # keep NULL if not found
            print(f"Warning: No match found for nationality '{nationalityname}'. Leaving NULL.")
        player[playerId]["nationality"] = nationalityCountryCode
        print("...done.")

        # positionId = getPositionId(conn, positionname)
        # print(positionId)
        # player[playerId]["position"] = positionId
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
        playerId,
        player[playerId]["firstname"],
        player[playerId]["lastname"],
        player[playerId]["birthdate"],
        player[playerId]["birthplace"],
        player[playerId]["birthcountrycode"],
        player[playerId]["nationality"],
        player[playerId]["heightcm"],
        player[playerId]["weightkg"],
       # player[playerId]["position"],
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newId = cur.fetchone()[0]
            print(f"Player {playerId} inserted with id {newId}.")

print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

print("Getting Player IDs...")
#playerId = int(input("Enter the Player ID:  "))
#playerId = 50870
#playerId = 6068
#print(f"You entered: {playerId}.")
#playerIds = [103046, 2460, 6068]
playerIds = []
getPlayers(headers, playerIds)
print(playerIds)

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

for playerId in playerIds:
    playerLookup(conn, playerId)



