import json
import http.client
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


def refereeWork(f, conn):
    # Get the raw referee info
    refereeRaw = f.get("referee")
    # If there is no information for referee in api, setting variables for None
    referee = None
    refereeCountry = None
    refId = 1
    if refereeRaw:
        # Strip it into parts and store as variables
        parts = [p.strip() for p in refereeRaw.split(",")]
        referee = parts[0] if len(parts) > 0 and parts[0] else None
        refereeCountry = parts[1] if len(parts) > 1 and parts[1] else None
        print(f"Referee: {referee}, Country: {refereeCountry}")
        # See if referee is in db
        with conn.cursor() as cur:
            cur.execute("SELECT concat_ws(' ', firstname, lastname) as fullname, id FROM public.referee")
            rows = cur.fetchall()
        existingReferees = {row[0]: row[1] for row in rows if row[0] is not None}
        print(existingReferees)
        # If referee is in db, get referee id
        if referee in existingReferees:
            refId = existingReferees[referee]
            print(f"Referee {referee} is already in the database, referee id: {refId}")
        else:
            ##if referee is not in db, add referee to db
            print(f"Referee {referee} is not in the database, adding referee to db.")
            ##split full name into two
            firstname, lastname = splitFullName(referee)
            print(f"Firstname: {firstname} Lastname: {lastname}")
            refereeCountryCodeMap = applyCountryCodes(conn, refereeCountry)
            print(refereeCountryCodeMap)
            refereeCountryCode = None
            if refereeCountryCodeMap:
                refereeCountryCode = next(iter(refereeCountryCodeMap.values()))
                print(f"Referee Country Code: {refereeCountryCode}")
            refId = insertRef(firstname, lastname, refereeCountryCode)
        ##switch out country w/ countrycode

    else:
        print("No referee information, referee will be \'None\'.")

    print(f"refId = {refId}.")
    return refId


def venueWork(f, conn):
    #Get venue api id
    venueRaw = f.get("venue")
    print(f"Venue: {venueRaw}")






# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

# Get fixture id, store it as a variable
#fixtureId = int(input("Enter the fixture ID:  "))
#fixtureId = 147926
#fixtureId = 147915
fixtureId = 147936
# Store path to fixture info in a variable, to be used w/ connection information
path = f"/fixtures?id={fixtureId}"

# Get api info on fixture, store it as a variable, payload
apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
apiconn.request("GET", path, headers=headers)
res = apiconn.getresponse()
raw = res.read()
payload = json.loads(raw.decode("utf-8"))
print(payload)
# Strip out just the fixture info
fixture = ""
for item in payload.get("response", []):
    fixture = item.get("fixture") or {}
apiconn.close()
print(fixture)

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

# Looking into the referee info
refereeId = refereeWork(fixture, conn)
print(f"The referee id is {refereeId}.")

# Need to do venue before date and time
venueId = venueWork(fixture, conn)
print(f"The venue id is {venueId}.")


# Looking into the date and time info
# utcdatetime = fixture.get("date")
# print(f"The date and time is {utcdatetime}.")