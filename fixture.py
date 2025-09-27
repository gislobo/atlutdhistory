import json
import http.client
import psycopg2
import unicodedata
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from timezonefinderL import TimezoneFinder
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import datetime, timezone


# Minimal alias mapping for common names and Windows zones to IANA
_TZ_ALIAS_MAP = {
    # Canonical pass-through
    "america/new_york": "America/New_York",
    # US aliases
    "us/eastern": "America/New_York",
    "us/central": "America/Chicago",
    "us/mountain": "America/Denver",
    "us/pacific": "America/Los_Angeles",
    # Abbreviations (ambiguous; pick common US mappings)
    "est": "America/New_York",
    "edt": "America/New_York",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    # Common Windows display names -> IANA
    "(utc-05:00) eastern time (us & canada)": "America/New_York",
    "(utc-06:00) central time (us & canada)": "America/Chicago",
    "(utc-07:00) mountain time (us & canada)": "America/Denver",
    "(utc-08:00) pacific time (us & canada)": "America/Los_Angeles",
    "eastern standard time": "America/New_York",
    "central standard time": "America/Chicago",
    "mountain standard time": "America/Denver",
    "pacific standard time": "America/Los_Angeles",
}


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


def insertVenue(apiid, name, address, city, state, countrycode, capacity, surface, lat, long, tz):
    sql = """
        INSERT INTO public.venue (
            apifootballid, \
            name, \
            address, \
            city, \
            state, \
            countrycode, \
            capacity, \
            surface, \
            latitude, \
            longitude, \
            timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    params = (
        apiid,
        name,
        address,
        city,
        state,
        countrycode,
        capacity,
        surface,
        lat,
        long,
        tz,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newId = cur.fetchone()[0]
            print(f"Venue inserted with id {newId}.")

    return newId


def venueWork(f, conn): # f is fixture
    #Get venue api id
    venueRaw = f.get("venue")
    print(f"Venue: {venueRaw}")
    # single out the venue name
    venueName = venueRaw['name']
    # some initializing
    apiid = None
    address = ""
    city = ""
    state = ""
    countrycode = ""
    capacity = ""
    surface = ""
    tz = ""
    if venueRaw['id'] is None: # most venues in apifootball don't have an api id, at least fo the first few matches
        print("Venue is None.")
        # Check to see if Venue already exists anyway
        with conn.cursor() as cur: # creating a list (or dictionary?  tuple?) of all venues where api id is none
            cur.execute("SELECT name, id FROM public.venue WHERE apifootballid is NULL")
            rows = cur.fetchall()
        # getting just the names of the venues into a list, leaving the gislobo id behind
        existingNoneVenues = {row[0]: row[1] for row in rows if row[0] is not None}
        print(f"All existing venues w/o api id:  {existingNoneVenues}")
        if venueName in existingNoneVenues: # running through the list to see if venue name is in the list
            print(f"Venue {venueName} is already in the database, no need to proceed.")
            print(f"Venue id: {existingNoneVenues[venueName]}")
            with conn.cursor() as cur: # getting the timezone of the already existing venue
                cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (existingNoneVenues[venueName],))
                tz = cur.fetchone()[0]
            return existingNoneVenues[venueName], tz # if it is, we're done, return the id
        else:  # else we have some work to do
            print("not in db, going to add it in")
            # solicit information
            address = input(f"Enter the street address for {venueName}: ")
            city = input(f"Enter the city for {venueName}: ")
            state = input(f"Enter the state for {venueName}: ")
            countrycode = input(f"Enter the country code for {venueName}: ")
            capacity = input(f"Enter the capacity for {venueName}: ")
            surface = input(f"Enter the surface for {venueName}: ")

            # create a function that finds and inserts lat and long based on address
            def geocode_address(a: str) -> tuple[float, float] | None:
                """
                Returns (latitude, longitude) in decimal degrees for the given address,
                or None if not found.
                """
                geolocator = Nominatim(user_agent="gislobo")  # set a descriptive app name
                geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # be polite with OSM
                location = geocode(address, exactly_one=True, addressdetails=False)
                if location is None:
                    return None
                return location.latitude, location.longitude


            geocodeaddr = f"{address}, {city}, {state}"
            coords = geocode_address(geocodeaddr)
            if coords:
                lat, lon = coords
                print(f"Latitude: {lat}, Longitude: {lon}")
            else:
                lat = None
                lon = None
                print("Address not found.")

            # create a function that finds and inserts timezone based on lat/long
            tf = TimezoneFinder()
            tz = tf.timezone_at(lng=lon, lat=lat)
            print(f"The timezone is {tz}.")

            # call insertVenue
            thevenueid = insertVenue(apiid, venueName, address, city, state, countrycode, capacity, surface, lat, lon, tz)
            return thevenueid, tz


def _normalize_tz_key(key: str | None) -> str | None:
    if not key or not str(key).strip():
        return None
    s = str(key).strip()
    # If it's already an IANA-like path with slash, leave case as-is for ZoneInfo
    if "/" in s:
        return s
    # Otherwise normalize for alias lookup
    return " ".join(s.lower().split())


def _alias_to_iana(key: str) -> str:
    norm = _normalize_tz_key(key)
    if not norm:
        return key
    # If looks like IANA (contains '/'), return as-is
    if "/" in key:
        return key
    return _TZ_ALIAS_MAP.get(norm, key)


def _safe_zoneinfo(key: str) -> ZoneInfo | None:
    iana = _alias_to_iana(key)
    try:
        return ZoneInfo(iana)
    except ZoneInfoNotFoundError:
        return None


def to_tz_from_utc(utc_dt, target_tz: str) -> datetime:
    """
    Convert a UTC datetime (str or datetime) to the given IANA timezone.

    - utc_dt can be:
      - ISO 8601 string (e.g., "2025-09-27T14:30:00Z" or "2025-09-27T14:30:00+00:00")
      - naive datetime assumed to be UTC
      - aware datetime (will be converted from its timezone to target)

    Returns an aware datetime in the target timezone. If the requested timezone
    cannot be loaded on this system, falls back to the local timezone.
    """
    if isinstance(utc_dt, str):
        s = utc_dt.strip()
        if not s:
            raise ValueError("utc_dt string is empty")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception as e:
            raise ValueError(f"Unable to parse datetime string: {utc_dt!r}") from e
    elif isinstance(utc_dt, datetime):
        dt = utc_dt
    else:
        raise TypeError("utc_dt must be a str or datetime")

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    tz = _safe_zoneinfo(target_tz)
    if tz is None:
        # Graceful fallback: system local timezone
        return dt.astimezone()  # converts to local time
    return dt.astimezone(tz)



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
fixtureId = 147915
#fixtureId = 147936
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
# refereeId = refereeWork(fixture, conn)
# print(f"The referee id is {refereeId}.")

# Need to do venue before date and time
venueId, fixturetimezone = venueWork(fixture, conn)
print(f"The venue id is {venueId}.")
print(f"The timezone is {fixturetimezone}.")


# Looking into the date and time info
utcdatetime = fixture.get("date")
localtime = to_tz_from_utc(utcdatetime, fixturetimezone)
print(utcdatetime)
print(localtime)
