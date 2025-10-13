import http.client
import json
import psycopg2
import unicodedata


#used
def loadHeaders(headersPath="headers.json"):
    with open(headersPath, "r", encoding="utf-8") as f:
        return json.load(f)


#used
def loadDbConfig(configPath="dbconfig.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return{
        "host": cfg.get("host"),
        "port": int(cfg.get("port")),
        "dbname": cfg.get("dbname"),
        "user": cfg.get("user"),
        "password": cfg.get("password")
    }


#used
def getReferee(headers):
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")
    #fixtureId = int(input("Enter the Fixture ID:  "))
    #fixtureId = 147926
    #fixtureId = 147915
    fixtureId = 147936
    path = f"/fixtures?id={fixtureId}"

    conn.request("GET", path, headers=headers)
    res = conn.getresponse()
    raw = res.read()

    payload = json.loads(raw.decode("utf-8"))
    print(payload)

    for item in payload.get("response", []):
        fixture = item.get("fixture") or {}
        refereeRaw = fixture.get("referee")
        if refereeRaw:
            parts = [p.strip() for p in refereeRaw.split(",")]
            referee = parts[0] if len(parts) > 0 and parts[0] else None
            refereeCountry = parts[1] if len(parts) > 1 and parts[1] else None
        else:
            referee = None
            refereeCountry = None
    conn.close()
    return referee, refereeCountry


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


#used
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


#used
def refereeLookup(conn, r):
    with conn.cursor() as cur:
        cur.execute("SELECT concat_ws(' ', firstname, lastname) as fullname FROM public.referee")
        rows = cur.fetchall()
    existingReferees = {row[0] for row in rows if row[0] is not None}
    print(existingReferees)

    addToDb = True
    if r in existingReferees:
        print(f"Referee {r} is already in the database, exiting.")
        addToDb = False
        return addToDb, addToDb
    elif r is None:
        print(f"Referee is None ({r}), exiting.")
        addToDb = False
        return addToDb, r
    else:
        print(f"Referee {r} is not in the database, proceeding.")
        first, last = splitFullName(r)
        print(f"Firstname: {first}, Lastname: {last}")
        return first, last


#used
def splitFullName(fullname: str) -> tuple[str | None, str | None]:
    if not fullname or not fullname.strip():
        return None, None

    # Normalize whitespace
    tokens = fullname.strip().split()

    # Single token
    if len(tokens) == 1:
        return tokens[0], None

    # Common suffixes (case-insensitive, with punctuation ignored)
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v", "phd", "md", "esq"}
    def norm(t: str) -> str:
        return "".join(ch for ch in t.lower() if ch.isalnum())

    # Strip trailing suffixes
    while len(tokens) > 1 and norm(tokens[-1]) in suffixes:
        tokens.pop()

    # If everything got stripped to one token
    if len(tokens) == 1:
        return tokens[0], None

    # Surname particles that often belong with the last name
    particles = {"da", "de", "del", "della", "der", "di", "dos", "du", "la", "le",
                 "van", "von", "bin", "al", "ibn", "mac", "mc", "st", "st.", "ter"}

    # Start with last token as core last name
    lastParts = [tokens[-1]]

    # Pull preceding particles into the last name
    i = len(tokens) - 2
    while i >= 1 and norm(tokens[i]) in particles:
        lastParts.insert(0, tokens[i])
        i -= 1

    firstname = tokens[0]
    lastname = " ".join(lastParts) if lastParts else None

    # If anything remains between first and lastParts, treat as middle names; attach to last name
    if i >= 1:
        middle = " ".join(tokens[1:i+1])
        lastname = f"{middle} {lastname}" if lastname else middle

    return firstname, lastname


#used
def insertRef(first, last, code):
    sql = """
        INSERT INTO public.referee (firstname, \
                                   lastname, \
                                   countrycode) 
            VALUES (%s, %s, %s) RETURNING id
    """
    params = (
        first,
        last,
        code,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newId = cur.fetchone()[0]
            print(f"Ref inserted with id {newId}.")
    return newId


print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")
print("Loading DB config...")
db = loadDbConfig("dbconfig.json")
print("...DB config loaded.")

print("Getting Referee...")
referee, refereeCountry = getReferee(headers)
print(f"Referee: {referee}, Country: {refereeCountry}")

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

firstname, lastname = refereeLookup(conn, referee)
refId = 0
if firstname:
    print(f"Proceeding....")
    refereeCountryCodeMap = applyCountryCodes(conn, refereeCountry)
    print(refereeCountryCodeMap)
    refereeCountryCode = None
    if refereeCountryCodeMap:
        refereeCountryCode = next(iter(refereeCountryCodeMap.values()))
        print(f"Referee Country Code: {refereeCountryCode}")
    refId = insertRef(firstname, lastname, refereeCountryCode)
elif lastname is None:
    refId = 1
else:

print(refId)


