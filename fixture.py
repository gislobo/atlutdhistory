import json
import http.client
import sys
import psycopg2
import unicodedata
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from timezonefinderL import TimezoneFinder
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import datetime, timezone, date
from typing import Optional


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
    print(f"Referee raw info:  {refereeRaw}")
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
        # getting just the names of the venues into a dictionary
        existingNoneVenues = {row[0]: row[1] for row in rows if row[0] is not None}
        print(f"All existing venues w/o api id:  {existingNoneVenues}")
        if venueName in existingNoneVenues: # running through the list to see if venue name is in the list
            print(f"Venue {venueName} is already in the database, no need to proceed.")
            print(f"Venue id: {existingNoneVenues[venueName]}")
            with conn.cursor() as cur: # getting the timezone of the already existing venue
                cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (existingNoneVenues[venueName],))
                tz = cur.fetchone()[0]
            return existingNoneVenues[venueName], tz # if it is, we're done, return the id
        elif venueName == 'Mercedes-Benz Stadium (Atlanta, Georgia)':
            return 4, "America/New_York"
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
            def geocode_address(a: str) -> Optional[tuple[float, float]]:
                """
                Returns (latitude, longitude) in decimal degrees for the given address,
                or None if not found or on transient network errors.
                """
                address = (a or "").strip()
                if not address:
                    return None

                # Configure a descriptive user agent and a sensible timeout per request
                geolocator = Nominatim(user_agent="atlutdhistory-app/1.0 (contact: youremail@example.com)", timeout=10)

                # RateLimiter helps respect Nominatim usage policy; add retries and error swallowing
                geocode = RateLimiter(
                    geolocator.geocode,
                    min_delay_seconds=1.0,
                    max_retries=3,  # retry transient failures
                    error_wait_seconds=2.0,  # wait between retries on errors
                    swallow_exceptions=False,  # propagate so we can handle specific cases below
                )

                try:
                    # exactly_one True returns a single Location or None
                    location = geocode(address, exactly_one=True, addressdetails=False)
                except (GeocoderTimedOut, GeocoderUnavailable) as e:
                    # Network/service transient issue: return None gracefully
                    return None
                except GeocoderServiceError:
                    # Other geopy service errors (e.g., bad response)
                    return None
                except Exception:
                    # Any unexpected error: fail gracefully
                    return None

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
    else:
        print("Venue has an id in the api!!")
        return venueRaw['id'], tz


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
    print(f"in the second function, iana is {iana}")
    try:
        return ZoneInfo(iana)
    except ZoneInfoNotFoundError:
        print("zoneinfonotfounderror")
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
    print(f"function testing, look here, utc_dt is {utc_dt}")
    print(f"function testing, look here, target_tz is {target_tz}")
    if isinstance(utc_dt, str):
        s = utc_dt.strip()
        print(f"loop testing, look here, s is {s}")
        if not s:
            raise ValueError("utc_dt string is empty")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
            print("z thingy")
        try:
            dt = datetime.fromisoformat(s)
            print(f"loop, try testing, dt is {dt}")
        except Exception as e:
            raise ValueError(f"Unable to parse datetime string: {utc_dt!r}") from e
    elif isinstance(utc_dt, datetime):
        dt = utc_dt
    else:
        raise TypeError("utc_dt must be a str or datetime")

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
        print(f"naive testing, dt is {dt}")

    tz = _safe_zoneinfo(target_tz)
    print(f"tz testing, after save zone thingy, tz is {tz}")
    if tz is None:
        # Graceful fallback: system local timezone
        return dt.astimezone()  # converts to local time
    return dt.astimezone(tz)


def key_for_value(d, value):
    for k, v in d.items():
        if v == value:
            return k
    return None


def leaguework(lid, conn, lr):
    with conn.cursor() as cur:
        cur.execute("SELECT id, apifootballid from public.league")
        rows = cur.fetchall()
    existingleaguesdict = {row[0]: row[1] for row in rows if row[0] is not None}
    existingleagues = list(existingleaguesdict.values())
    print(f"All existing leagues: {existingleagues}")
    databaseid = ""
    if lid in existingleagues:
        print(f"Yes, {lid}")
        databaseid = key_for_value(existingleaguesdict, lid)
        if lid == 253 and lr == 'Play-In Round - Finals':
            databaseid = 3
    else:
        print(f"API League ID {lid} is not in your database.")
        print("Please insert it and then give me the number.")
        databaseid = int(input("Enter the league ID:  "))
    return databaseid


def insertteam(aid, name, code, fdate):
    sql = """
        INSERT INTO public.team (apifootballid, \
                                 name, \
                                 countrycode, \
                                 foundeddate)
            VALUES (%s, %s, %s, %s) RETURNING id
    """
    params = (
        aid,
        name,
        code,
        fdate,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"Team inserted with id {newid}.")
    return newid


def teamwork(tid, conn, headers):
    with conn.cursor() as cur:
        cur.execute("SELECT id, apifootballid from public.team")
        rows = cur.fetchall()
    existingteamsdict = {row[0]: row[1] for row in rows if row[0] is not None}
    existingteams = list(existingteamsdict.values())
    #print(f"All existing teams: {existingteams}")
    databaseid = ""
    if tid in existingteams:
        print("Team already exists in database.")
        databaseid = key_for_value(existingteamsdict, tid)
    else:
        print(f"API Team ID {tid} is not in your database.")
        # do some fancy stuff to put team in database
        path = f"/teams?id={tid}"
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        apiconn.request("GET", path, headers=headers)
        res = apiconn.getresponse()
        raw = res.read()
        payload = json.loads(raw.decode("utf-8"))
        teaminfo = ""
        for item in payload.get("response", []):
            teaminfo = item.get("team") or {}
        apiconn.close()
        print(f"API Team ID {tid}: {teaminfo}")
        name = teaminfo.get("name")
        country = teaminfo.get("country")
        teamcountrycodemap = applyCountryCodes(conn, country)
        teamcountrycode = None
        if teamcountrycodemap:
            teamcountrycode = next(iter(teamcountrycodemap.values()))
        print(f"Team name: {name}")
        print(f"Team countrycode: {teamcountrycode}")
        teamfounded = str(teaminfo.get("founded"))
        # Normalize founded year
        def coerce_founded_to_date(value):
            if value is None:
                return None
            try:
                year = int(value)
                return date(year, 1, 1)
            except Exception:
                pass
            return None
        foundeddate = coerce_founded_to_date(teamfounded)
        databaseid = insertteam(tid, name, teamcountrycode, foundeddate)
    return databaseid


def fixturestatuswork(fs):
    long = fs.get("long")
    print(f"long: {long}")
    short = fs.get("short")
    print(f"short: {short}")
    elapsed = fs.get("elapsed")
    print(f"elapsed: {elapsed}")
    extra = fs.get("extra")
    print(f"extra: {extra}")

    fsboolean = True
    if long != "Match Finished":
        fsboolean = False
        print("Fixture status long doesn't match.")
    if short != "FT":
        fsboolean = False
        print("Fixture status short doesn't match.")
    if elapsed != 90:
        fsboolean = False
        print("Fixture status elapsed doesn't match.")
    if extra is not None:
        fsboolean = False
        print("Fixture status extra doesn't match.")
    if fsboolean:
        return 1

    fsboolean1 = True
    if long != "Match Finished":
        fsboolean1 = False
        print("Fixture status long doesn't match.")
    if short != "PEN":
        fsboolean1 = False
        print("Fixture status short doesn't match.")
    if elapsed != 120:
        fsboolean1 = False
        print("Fixture status elapsed doesn't match.")
    if extra is not None:
        fsboolean1 = False
        print("Fixture status extra doesn't match.")
    if fsboolean1:
        return 2


def _parse_api_utc(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# Load headers from json file for use in api requests
print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")

# Load DB config from json file for use in connecting to database
print("Loading DB config...")
db = loadDbConfig("dbconfig.json")
print("...DB config loaded.")

# Get fixture id, store it as a variable
fixtureId = int(input("Enter the fixture ID:  "))
#fixtureId = 147926
######fixtureId = 147915
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
leagueinfo = ""
for item in payload.get("response", []):
    fixture = item.get("fixture") or {}
    leagueinfo = item.get("league") or {}
    teamsinfo = item.get("teams") or {}
    goalsinfo = item.get("goals") or {}
    scoreinfo = item.get("score") or {}
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

# Run a check to see if that fixture id is already in the database
with conn.cursor() as cur:
    cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (fixtureId,))
    existingfixtures = cur.fetchall()
existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if existingfixture[0] is not None}
print(f"Existing fixtures: {existingfixturesdict}")
if fixtureId in existingfixturesdict:
    print(f"Fixture {fixtureId} is already in the database.")
    print(f"Fixture id is {existingfixturesdict[fixtureId]}.")
    sys.exit(0)


# Referee info
refereeId = refereeWork(fixture, conn)
print(f"The referee id is {refereeId}.")

# Venue info
venueId, fixturetimezone = venueWork(fixture, conn)
print(f"The venue id is {venueId}.")
print(f"The timezone is {fixturetimezone}.")

# Date and time info
utcdatetime_str = fixture.get("date")
localtime_aware = to_tz_from_utc(utcdatetime_str, fixturetimezone)
atlantatimezone = "America/New_York"
atlantatime_aware = localtime_aware if fixturetimezone == atlantatimezone else to_tz_from_utc(utcdatetime_str, atlantatimezone)


# League info
leagueapiid = leagueinfo.get("id")
leagueround = leagueinfo.get("round")
print(f"API League ID: {leagueapiid}.")
print(f"Round: {leagueround}.")
leagueid = leaguework(leagueapiid, conn, leagueround)
print(f"The league id is {leagueid}.")

# Team info
homeinfo = teamsinfo.get("home") or {}
awayinfo = teamsinfo.get("away") or {}
hometeamapiid = homeinfo.get("id")
awayteamapiid = awayinfo.get("id")
print(f"Home team api id: {hometeamapiid}.")
print(f"Away team api id: {awayteamapiid}.")
hometeamid = teamwork(hometeamapiid, conn, headers)
awayteamid = teamwork(awayteamapiid, conn, headers)
print(f"Home team id:  {hometeamid}.")
print(f"Away team id:  {awayteamid}.")

# Fixturestatus
fixturestatus = fixture.get("status")
print(f"Fixture status: {fixturestatus}.")
fixturestatusid = fixturestatuswork(fixturestatus)
print(f"Fixture status id is {fixturestatusid}.")

# Goals info
homegoals = goalsinfo.get("home")
awaygoals = goalsinfo.get("away")
print(f"home goals = {homegoals}.")
print(f"away goals = {awaygoals}.")

# Fixturewinner
print(f"home info {homeinfo}.")
print(f"away info {awayinfo}.")
homewinner = homeinfo.get("winner")
awaywinner = awayinfo.get("winner")
print(f"homewinner = {homewinner}.")
print(f"awaywinner = {awaywinner}.")
fixturewinner = None
if homewinner:
    print("home won")
    fixturewinner = hometeamid
elif awaywinner:
    print("away won")
    fixturewinner = awayteamid
else:
    print("Ended in a draw")
    fixturewinner = 8
print(f"fixturewinner = {fixturewinner}.")

# Score info
halftimeinfo = scoreinfo.get("halftime")
halftimehome = halftimeinfo.get("home")
halftimeaway = halftimeinfo.get("away")
print(f"halftimehome = {halftimehome}.")
print(f"halftimeaway = {halftimeaway}.")

fulltimeinfo = scoreinfo.get("fulltime")
fulltimehome = fulltimeinfo.get("home")
fulltimeaway = fulltimeinfo.get("away")
print(f"fulltimehome = {fulltimehome}.")
print(f"fulltimeaway = {fulltimeaway}.")

extratimeinfo = scoreinfo.get("extratime")
extratimehome = extratimeinfo.get("home")
extratimeaway = extratimeinfo.get("away")
print(f"extratimehome = {extratimehome}.")
print(f"extratimeaway = {extratimeaway}.")

penaltyinfo = scoreinfo.get("penalty")
penaltyhome = penaltyinfo.get("home")
penaltyaway = penaltyinfo.get("away")
print(f"penaltyhome = {penaltyhome}.")
print(f"penaltyaway = {penaltyaway}.")

# For timestamp (without time zone) columns, use naive "wall times"
utcdatetime = _parse_api_utc(utcdatetime_str).replace(tzinfo=None)     # wall time in UTC
localtime = localtime_aware.replace(tzinfo=None)                        # wall time in venue tz
atlantatime = atlantatime_aware.replace(tzinfo=None)                    # wall time in Atlanta

print(f"before insert, utcdatetime_str is {utcdatetime_str}")
print(f"before insert, localtime is {localtime}")
print(f"before insert, atlantatime is {atlantatime}")
utcdatetime = _parse_api_utc(utcdatetime_str)
print(f"after parsing thingy, utcdatetime is {utcdatetime}")
#Insert fixture record
sql = """
INSERT INTO public.fixture (apisportsid, \
                            referee, \
                            utcdatetime, \
                            localdatetime, \
                            venue, \
                            league, \
                            hometeam, \
                            awayteam, \
                            fixturestatus, \
                            fixturewinner, \
                            homegoal, \
                            awaygoal, \
                            halftimehomescore, \
                            halftimeawayscore, \
                            fulltimehomescore, \
                            fulltimeawayscore, \
                            extratimehomescore, \
                            extratimeawayscore, \
                            penaltyhome, \
                            penaltyaway, \
                            atlantatime)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
"""
params = (
    fixtureId,
    refereeId,
    utcdatetime,
    localtime,
    venueId,
    leagueid,
    hometeamid,
    awayteamid,
    fixturestatusid,
    fixturewinner,
    homegoals,
    awaygoals,
    halftimehome,
    halftimeaway,
    fulltimehome,
    fulltimeaway,
    extratimehome,
    extratimeaway,
    penaltyhome,
    penaltyaway,
    atlantatime,
)

with conn:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        databasefixtureid = cursor.fetchone()[0]
        print(f"Database fixture id: {databasefixtureid}.")

print("and you're done")