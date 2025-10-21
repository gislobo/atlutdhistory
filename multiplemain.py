import http.client
import json
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


def splitfullname(fullname: str) -> tuple[str | None, str | None]:
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


def insertref(first, last, code, c):
    sql = """
        INSERT INTO public.referee (firstname, \
                                   lastname, \
                                   countrycode, \
                                   data_source, \
                                   created_by) 
            VALUES (%s, %s, %s, %s, %s) RETURNING id
    """
    ds = 'API-Football'
    cb = 'gislobo'
    params = (
        first,
        last,
        code,
        ds,
        cb,
    )

    with c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"Ref inserted with id {newid}.")
    return newid


def refereework(f, conn):
    # Get the raw referee info
    refereeraw = f.get("referee")
    print(f"Referee raw info:  {refereeraw}")
    # If there is no information for referee in api, setting variables for None
    referee = None
    refereecountry = None
    refid = 1
    if refereeraw:
        # Strip it into parts and store as variables
        parts = [p.strip() for p in refereeraw.split(",")]
        referee = parts[0] if len(parts) > 0 and parts[0] else None
        refereecountry = parts[1] if len(parts) > 1 and parts[1] else None
        print(f"Referee: {referee}, Country: {refereecountry}")
        # See if referee is in db
        with conn.cursor() as cur:
            cur.execute("SELECT concat_ws(' ', left(firstname, 1), lastname) as fullname, id FROM public.referee")
            rows = cur.fetchall()
        existingreferees = {row[0]: row[1] for row in rows if row[0] is not None}
        print(existingreferees)

        # Normalize referee name by removing period after initial for comparison
        referee_normalized = referee.replace('.', '') if referee else None

        # If referee is in db, get referee id
        if referee_normalized in existingreferees:
            refid = existingreferees[referee_normalized]
            print(f"Referee {referee} is already in the database, referee id: {refid}")
        else:
            ##if referee is not in db, add referee to db
            print(f"Referee {referee} is not in the database, adding referee to db.")
            ##split full name into two
            firstname, lastname = splitfullname(referee)
            print(f"Firstname: {firstname} Lastname: {lastname}")
            refereecountrycodemap = applycountrycodes(conn, refereecountry)
            print(refereecountrycodemap)
            refereecountrycode = None
            if refereecountrycodemap:
                refereecountrycode = next(iter(refereecountrycodemap.values()))
                print(f"Referee Country Code: {refereecountrycode}")
            refid = insertref(firstname, lastname, refereecountrycode, conn)
        ##switch out country w/ countrycode

    else:
        print("No referee information, referee will be \'None\'.")

    print(f"refid = {refid}.")
    return refid


def insertvenue(apiid, name, address, city, state, countrycode, capacity, surface, lat, long, tz, c):
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
            timezone, \
            data_source, \
            created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    ds = 'API-Football'
    cb = 'gislobo'
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
        ds,
        cb,
    )

    with c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"Venue inserted with id {newid}.")

    return newid


def venuework(f, conn): # f is fixture
    #Get venue api id
    venueraw = f.get("venue")
    print(f"Venue: {venueraw}")
    # single out the venue name
    venuename = venueraw['name']
    # some initializing
    apiid = None
    address = ""
    city = ""
    state = ""
    countrycode = ""
    capacity = ""
    surface = ""
    tz = ""
    if venueraw['id'] is None: # most venues in apifootball don't have an api id, at least for the first few matches
        print("Venue is None.")
        # Check to see if Venue already exists anyway
        with conn.cursor() as cur: # creating a list (or dictionary?  tuple?) of all venues where api id is none
            cur.execute("SELECT name, id FROM public.venue WHERE apifootballid is NULL")
            rows = cur.fetchall()
        # getting just the names of the venues into a dictionary
        existingnonevenues = {row[0]: row[1] for row in rows if row[0] is not None}
        print(f"All existing venues w/o api id:  {existingnonevenues}")
        if venuename in existingnonevenues: # running through the list to see if venue name is in the list
            print(f"Venue {venuename} is already in the database, no need to proceed.")
            print(f"Venue id: {existingnonevenues[venuename]}")
            with conn.cursor() as cur: # getting the timezone of the already existing venue
                cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (existingnonevenues[venuename],))
                tz = cur.fetchone()[0]
            return existingnonevenues[venuename], tz # if it is, we're done, return the id
        elif venuename == 'Mercedes-Benz Stadium (Atlanta, Georgia)':
            return 4, "America/New_York"
        else:  # else we have some work to do
            print("not in db, going to add it in")
            # solicit information
            yesno = input("Is this venue an already existing venue that has been renamed? (y/n): ")
            if yesno == "y":
                changedvenueid = input("Enter the database id of the venue:  ")
                # FIXED: Retrieve timezone for the existing venue
                with conn.cursor() as cur:
                    cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (changedvenueid,))
                    tz = cur.fetchone()[0]
                return changedvenueid, tz
            address = input(f"Enter the street address for {venuename}: ")
            city = input(f"Enter the city for {venuename}: ")
            state = input(f"Enter the state for {venuename}: ")
            countrycode = input(f"Enter the country code for {venuename}: ")
            capacity = input(f"Enter the capacity for {venuename}: ")
            surface = input(f"Enter the surface for {venuename}: ")

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
            # FIXED: Only attempt timezone lookup if we have valid coordinates
            if lat is not None and lon is not None:
                tf = TimezoneFinder()
                tz = tf.timezone_at(lng=lon, lat=lat)
                print(f"The timezone is {tz}.")
            else:
                print("Cannot determine timezone without valid coordinates.")
                tz = input(f"Please enter the timezone manually (e.g., 'America/New_York'): ")
                if not tz or not tz.strip():
                    tz = None
                    print("No timezone provided, setting to None.")

            # call insertvenue
            thevenueid = insertvenue(apiid, venuename, address, city, state, countrycode, capacity, surface, lat, lon,
                                     tz, conn)
            return thevenueid, tz
    else:
        print("Venue has an id in the api!!")

        # Check to see if the venuid is already in the database
        with conn.cursor() as cur:
            cur.execute("select id, apifootballid from public.venue where apifootballid is not null")
            rows = cur.fetchall()
        existingapivenues = {row[1]: row[0] for row in rows if row[1] is not None}
        print(f"All existing apivenues: {existingapivenues}")
        if venueraw['id'] in existingapivenues:
            print(f"Venue {venueraw['id']} is already in the database, no need to proceed.")
            print(f"Venue databaseid = {existingapivenues[venueraw['id']]}.")
            with conn.cursor() as cur:
                cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (existingapivenues[venueraw['id']],))
                tz = cur.fetchone()[0]
            return existingapivenues[venueraw['id']], tz
        else:
            yesno = input(f"Is this a venue already in the database without an api id? (y/n): ")
            if yesno == 'y':
                thevenueid = int(input("Enter the database id of the venue: "))
                with conn.cursor() as cur:
                    cur.execute("SELECT timezone FROM public.venue WHERE id = %s", (thevenueid,))
                    tz = cur.fetchone()[0]
                return thevenueid, tz
            else:
                print("We must create a new venue.")
                apiid = int(input("Enter the api id for the venue: "))
                venuename = input("Enter the venue name: ")
                address = input("Enter the street address: ")
                city = input("Enter the city: ")
                state = input("Enter the state: ")
                countrycode = input("Enter the country code: ")
                capacity = input("Enter the capacity: ")
                surface = input("Enter the surface: ")

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
                    geolocator = Nominatim(user_agent="atlutdhistory-app/1.0 (contact: youremail@example.com)",
                                           timeout=10)

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
                # FIXED: Only attempt timezone lookup if we have valid coordinates
                if lat is not None and lon is not None:
                    tf = TimezoneFinder()
                    tz = tf.timezone_at(lng=lon, lat=lat)
                    print(f"The timezone is {tz}.")
                else:
                    print("Cannot determine timezone without valid coordinates.")
                    tz = input(f"Please enter the timezone manually (e.g., 'America/New_York'): ")
                    if not tz or not tz.strip():
                        tz = None
                        print("No timezone provided, setting to None.")

                thevenueid = insertvenue(apiid, venuename, address, city, state, countrycode, capacity, surface, lat,
                                         lon,
                                         tz, conn)
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
    print(f"in the second function, iana is {iana}")
    # Validate that iana is not empty or None before creating ZoneInfo
    if not iana or not iana.strip():
        print("Invalid timezone: empty or None, returning None")
        return None
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
    databaseid = None
    if lid in existingleagues:
        print(f"Yes, {lid}")
        databaseid = key_for_value(existingleaguesdict, lid)
        if lid == 253 and lr == 'Play-In Round - Finals':
            databaseid = 3
        if lid == 253 and lr == 'MLS Cup - Conference Semi-finals':
            databaseid = 3
        if lid == 253 and lr == 'MLS Cup - Conference Finals':
            databaseid = 3
        if lid == 253 and lr == 'MLS Cup - Final':
            databaseid = 3
        if lid == 253 and lr == 'Play-In Round':
            databaseid = 3
        if lid == 253 and lr == 'MLS Cup - Round 1':
            databaseid = 3
    else:
        print(f"API League ID {lid} is not in your database.")
        print("Please insert it and then give me the number.")
        databaseid = int(input("Enter the league ID:  "))
    return databaseid


def insertteam(aid, name, code, fdate, c):
    sql = """
        INSERT INTO public.team (apifootballid, \
                                 name, \
                                 countrycode, \
                                 foundeddate, \
                                 data_source, \
                                 created_by)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
    """
    ds = 'API-Football'
    cb = 'gislobo'
    params = (
        aid,
        name,
        code,
        fdate,
        ds,
        cb,
    )

    with c:
        with c.cursor() as cur:
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
        teamcountrycodemap = applycountrycodes(conn, country)
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
        databaseid = insertteam(tid, name, teamcountrycode, foundeddate, conn)
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

    
def loadheaders(headerspath="headers.json"):
    with open(headerspath, "r", encoding="utf-8") as f:
        return json.load(f)


def loaddbconfig(configpath="dbconfig.json"):
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
        ds = 'API-Football'
        cb = 'gislobo'
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.position (position, data_source, created_by) VALUES (%s, %s, %s) RETURNING id",
                (positionname, ds, cb,),
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
                                   weightkg, \
                                   data_source, \
                                   created_by) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    ds = 'API-Football'
    cb = 'gislobo'
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
        ds,
        cb,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"Player {playerid} inserted with id {newid}.")


def players(payload, headers, conn):
    playerids = []
    print(f"Getting player IDs...")
    getplayers(payload, playerids)
    print(f"Player IDs:  {playerids}.")

    for playerid in playerids:
        playerlookup(headers, conn, playerid)


def fixturefunction(payload, f, headers, conn):
    fixtureinfo = None
    leagueinfo = None
    teamsinfo = None
    goalsinfo = None
    scoreinfo = None
    for item in payload.get("response", []):
        fixtureinfo = item.get("fixture") or {}
        leagueinfo = item.get("league") or {}
        teamsinfo = item.get("teams") or {}
        goalsinfo = item.get("goals") or {}
        scoreinfo = item.get("score") or {}
    print(f"Fixture:  {fixtureinfo}.")
    print(f"League:  {leagueinfo}.")
    print(f"Teams:  {teamsinfo}.")
    print(f"Goals:  {goalsinfo}.")
    print(f"Score:  {scoreinfo}.")

    # Run a check to see if that fixture id is already in the database, if it is, exit the function
    with conn.cursor() as cur:
        cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (f,))
        existingfixtures = cur.fetchall()
    existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if
                            existingfixture[0] is not None}
    print(f"Existing fixtures: {existingfixturesdict}")
    if f in existingfixturesdict:
        print(f"Fixture {f} is already in the database.")
        print(f"Fixture id is {existingfixturesdict[f]}.")
        return

    # Referee info
    refereeId = refereework(fixtureinfo, conn)
    print(f"The referee id is {refereeId}.")

    # Venue info
    venueId, fixturetimezone = venuework(fixtureinfo, conn)
    print(f"The venue id is {venueId}.")
    print(f"The timezone is {fixturetimezone}.")

    # Date and time info
    utcdatetime_str = fixtureinfo.get("date")
    localtime_aware = to_tz_from_utc(utcdatetime_str, fixturetimezone)
    atlantatimezone = "America/New_York"
    atlantatime_aware = localtime_aware if fixturetimezone == atlantatimezone else to_tz_from_utc(utcdatetime_str,
                                                                                                  atlantatimezone)

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
    fixturestatus = fixtureinfo.get("status")
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
    utcdatetime = _parse_api_utc(utcdatetime_str).replace(tzinfo=None)  # wall time in UTC
    localtime = localtime_aware.replace(tzinfo=None)  # wall time in venue tz
    atlantatime = atlantatime_aware.replace(tzinfo=None)  # wall time in Atlanta

    print(f"before insert, utcdatetime_str is {utcdatetime_str}")
    print(f"before insert, localtime is {localtime}")
    print(f"before insert, atlantatime is {atlantatime}")
    utcdatetime = _parse_api_utc(utcdatetime_str)
    print(f"after parsing thingy, utcdatetime is {utcdatetime}")
    # Insert fixture record
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
                                      atlantatime, \
                                      data_source, \
                                      created_by)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
          RETURNING id \
          """
    ds = 'API-Football'
    cb = 'gislobo'
    params = (
        f,
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
        ds,
        cb,
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            databasefixtureid = cursor.fetchone()[0]
            print(f"Database fixture id: {databasefixtureid}.")

    print("and you're done")


def eventtypework(c, et, ed):
    # Get the event type information into a list of dictionaries
    print("Starting the eventypework function.")

    # Handle NULL eventdetail by providing a default value
    if ed is None or (isinstance(ed, str) and not ed.strip()):
        ed = "None"
        print(f"Event detail was None or empty, using default value: {ed}")

    with c.cursor() as cur:
        cur.execute("SELECT id, type, eventdetail FROM public.eventtype")
        rows = cur.fetchall()
    print(f"Rows of eventtype: {rows}")

    # Check to see if the event type exists in the database
    eventtypeexists = False
    dbeventtypeid = ""
    for row in rows:
        print(f"Row id {row[0]}, type {row[1]}, eventdetail {row[2]}")
        if row[1] == et and row[2] == ed:
            print(f"Found event type {et} and event detail {ed}.")
            dbeventtypeid = row[0]
            eventtypeexists = True
            break

    # If not in the database, add them in as a new row in public.eventtype
    if not eventtypeexists:
        print(f"Event type {et} and event detail {ed} not found in database.")
        ds = 'API-Football'
        cb = 'gislobo'
        with c:
            with c.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.eventtype (type, eventdetail, data_source, created_by) VALUES (%s, %s, %s, %s) RETURNING id",
                    (et, ed, ds, cb),
                )
                dbeventtypeid = cur.fetchone()[0]
                print(f"Event type {et} and event detail {ed} inserted with id {dbeventtypeid}.")

    print("Ending the eventypework function.")
    return dbeventtypeid


def eventfunction(payload, f, conn):
    ## Grab the database fixtureid
    with conn.cursor() as cur:
        cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (f,))
        existingfixtures = cur.fetchall()
    existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if
                            existingfixture[0] is not None}
    print(f"Existing fixtures: {existingfixturesdict}")
    databasefixtureid = ""
    if f in existingfixturesdict:
        databasefixtureid = existingfixturesdict[f]
        print(f"The database fixture id is {databasefixtureid}.")

    ## Check to see if the fixture has events already in the table
    with conn.cursor() as cur:
        cur.execute("SELECT fixtureid from public.fixtureevent")
        existingfixtureevents = cur.fetchall()
    existingids = {row[0] for row in existingfixtureevents}  # Extract first column into a set
    print(f"Existing fixture events: {existingids}")
    if databasefixtureid in existingids:
        print(f"The fixture {databasefixtureid} already has events in the database.")
        return

    ## Work out how to grab each event individually
    # API tells us how many events there are
    apiresults = payload.get("results") or {}
    print(f"The API tells us there are {apiresults} events.")

    # Ge the events into a list of dictionaries
    response = payload.get("response") or {}
    print(response)
    print(f"There are {len(response)} events in the response.")
    if len(response) == apiresults:
        print("The number of events in the response matches the number of events the API initially tells us there are.")
    else:
        print(
            "Something is wrong, the number of events in the response doesn't match the number of events the API tells us there are.")
        sys.exit(0)
    count = 0
    for event in response:
        count += 1
        print(f"Event {count}:")
        print(event)

        ## Event type work
        # Get type, detail, and comments into variables
        eventtype = event.get("type")
        eventdetail = event.get("detail")
        eventcomments = event.get("comments")
        print(f"Event type: {eventtype}")
        print(f"Event detail: {eventdetail}")
        print(f"Event comments: {eventcomments}")

        # Write a function to get the database id for the event type
        eventypeid = eventtypework(conn, eventtype, eventdetail)
        print(f"Event type id: {eventypeid}")
        print("")

        ## Time elapsed and extratimeelapsed
        # Get time info per event into their respective variables
        timeinfo = event.get("time") or {}
        print(f"Time info: {timeinfo}")
        elapsed = timeinfo.get("elapsed")
        extra = timeinfo.get("extra")
        print(f"Elapsed time: {elapsed}")
        print(f"Extra time elapsed: {extra}")
        print("")

        ## Get database team id
        teaminfo = event.get("team") or {}
        apiteamid = teaminfo.get("id")
        print(f"apiteamid: {apiteamid}")
        databaseteamid = ""
        with conn.cursor() as cur:
            cur.execute("SELECT id from public.team WHERE apifootballid = %s", (apiteamid,))
            databaseteamid = cur.fetchone()[0]
        print(f"Database team id: {databaseteamid}")
        print("")

        ## Get database player id
        playerinfo = event.get("player") or {}
        apiplayerid = playerinfo.get("id")
        print(f"apiplayerid: {apiplayerid}")
        databaseplayerid = None
        with conn.cursor() as cur:
            cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
            playerresult = cur.fetchone()
            if playerresult is None:
                print(f"WARNING:  Player with apifootballid {apiplayerid} not found in database.")
                databaseplayerid = int(input("Enter the database player id for the player: "))
            else:
                databaseplayerid = playerresult[0]
        print(f"Database player id: {databaseplayerid}")
        print("")

        ## Assist work (database player id)
        assistinfo = event.get("assist") or {}
        apiassistid = assistinfo.get("id")
        databaseassistid = ""
        if apiassistid is None:
            databaseassistid = None
            print(f"Assist id is None.")
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiassistid,))
                assistresult = cur.fetchone()
                if assistresult is None:
                    print(f"WARNING:  Assist with apifootballid {apiassistid} not found in database.")
                    databaseassistid = int(input("Enter the database player id for the player: "))
                else:
                    databaseassistid = assistresult[0]
            print(f"Database assist id: {databaseassistid}")

        ## Load into database
        sql = """
              INSERT INTO public.fixtureevent (fixtureid, \
                                               eventtype, \
                                               eventcomments, \
                                               timeelapsed, \
                                               extratimeelapsed, \
                                               team, \
                                               player, \
                                               assist, \
                                               data_source, \
                                               created_by)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
              RETURNING id \
              """
        ds = 'API-Football'
        cb = 'gislobo'
        params = (
            databasefixtureid,
            eventypeid,
            eventcomments,
            elapsed,
            extra,
            databaseteamid,
            databaseplayerid,
            databaseassistid,
            ds,
            cb,
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                eventid = cur.fetchone()[0]
                print(f"Event inserted with id {eventid}.")

        print("")
        print("---------------------------------")


def percentstringtofloat(str):
    """Convert a percentage string like '61%' to a float like 61.0"""
    if str is None or str == "":
        return None
    return float(str.strip('%'))


def statisticsfunction(payload, f, conn):
    ## Grab the database fixtureid
    with conn.cursor() as cur:
        cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (f,))
        existingfixtures = cur.fetchall()
    existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if
                            existingfixture[0] is not None}
    print(f"Existing fixtures: {existingfixturesdict}")
    databasefixtureid = None
    if f in existingfixturesdict:
        databasefixtureid = existingfixturesdict[f]
        print(f"The database fixture id is {databasefixtureid}.")

    ## See if the fixture already has statistics
    with conn.cursor() as cur:
        cur.execute("select dbfixtureid from public.fixturestatistics")
        existingfixturestatisticsidsfetchall = cur.fetchall()
    existingfixturestatisticsids = {row[0] for row in existingfixturestatisticsidsfetchall}
    if databasefixtureid in existingfixturestatisticsids:
        print(f"The fixture {databasefixtureid} already has statistics in the database, exiting.")
        return

    ## Work out how to grab each team's statistics individually
    # API tells us how many events there are
    apiresults = payload.get("results") or {}
    print(f"The API tells us there are {apiresults} events.")

    # Get the events into a list of dictionaries
    response = payload.get("response") or {}
    print(response)
    print(f"There are {len(response)} events in the response.")
    if len(response) == apiresults:
        print("The number of events in the response matches the number of events the API initially tells us there are.")
    else:
        print(
            "Something is wrong, the number of events in the response doesn't match the number of events the API tells us there are.")
        sys.exit(0)

    count = 0
    for event in response:
        count += 1
        print(f"Event {count}:")
        print(event)

        ## Get db team id
        teaminfo = event.get("team") or {}
        apiteamid = teaminfo.get("id")
        print(f"apiteamid: {apiteamid}")
        with conn.cursor() as cur:
            cur.execute("SELECT apifootballid, id from public.team where apifootballid = %s", (apiteamid,))
            existingteams = cur.fetchall()
        existingteamsdict = {existingteam[0]: existingteam[1] for existingteam in existingteams if
                             existingteam[0] is not None}
        databaseteamid = None
        if apiteamid in existingteamsdict:
            databaseteamid = existingteamsdict[apiteamid]
            print(f"The database team id is {databaseteamid}.")
        else:
            print(f"API Team ID {apiteamid} is not in your database.")
            sys.exit(0)

        ## Get stats into variables
        # Initialize variables
        shotsongoal = None
        shotsoffgoal = None
        totalshots = None
        blockedshots = None
        shotsinsidebox = None
        shotsoutsidebox = None
        fouls = None
        cornerkicks = None
        offsides = None
        ballpossessionstr = None
        ballpossession = None
        yellowcards = None
        redcards = None
        goalkeepersaves = None
        totalpasses = None
        passesaccurate = None
        apistats = event.get("statistics") or {}
        print(f"Stats: {apistats}")
        print(f"length of stats: {len(apistats)}")
        for stat in apistats:
            print(f"stat: {stat}")
            stattype = stat.get("type")
            statvalue = stat.get("value")
            if stattype == 'Shots on Goal':
                shotsongoal = statvalue
                print(f"Shots on goal: {shotsongoal}")
            if stattype == 'Shots off Goal':
                shotsoffgoal = statvalue
                print(f"Shots off goal: {shotsoffgoal}")
            if stattype == 'Total Shots':
                totalshots = statvalue
                print(f"Total shots: {totalshots}")
            if stattype == 'Blocked Shots':
                blockedshots = statvalue
                print(f"Blocked shots: {blockedshots}")
            if stattype == 'Shots insidebox':
                shotsinsidebox = statvalue
                print(f"Shots inside box: {shotsinsidebox}")
            if stattype == 'Shots outsidebox':
                shotsoutsidebox = statvalue
                print(f"Shots outside box: {shotsoutsidebox}")
            if stattype == 'Fouls':
                fouls = statvalue
                print(f"Fouls: {fouls}")
            if stattype == 'Corner Kicks':
                cornerkicks = statvalue
                print(f"Corner kicks: {cornerkicks}")
            if stattype == 'Offsides':
                offsides = statvalue
                print(f"Offsides: {offsides}")
            if stattype == 'Ball Possession':
                ballpossessionstr = statvalue
                print(f"Ball possession string: {ballpossessionstr}")
                # Converting ballpossession to a float
                print("Converting ballpossession to a float...")
                ballpossession = percentstringtofloat(ballpossessionstr)
                print(f"Ball possession percent: {ballpossession}")
            if stattype == 'Yellow Cards':
                yellowcards = statvalue
                print(f"Yellow cards: {yellowcards}")
            if stattype == 'Red Cards':
                redcards = statvalue
                print(f"Red cards: {redcards}")
            if stattype == 'Goalkeeper Saves':
                goalkeepersaves = statvalue
                print(f"Goalkeeper saves: {goalkeepersaves}")
            if stattype == 'Total passes':
                totalpasses = statvalue
                print(f"Total passes: {totalpasses}")
            if stattype == 'Passes accurate':
                passesaccurate = statvalue
                print(f"Passes accurate: {passesaccurate}")

        ## Load into database
        sql = """
              INSERT INTO public.fixturestatistics (dbfixtureid, \
                                                    dbteamid, \
                                                    shotsongoal, \
                                                    shotsoffgoal, \
                                                    totalshots, \
                                                    blockedshots, \
                                                    goalkeepersaves, \
                                                    shotsinsidebox, \
                                                    shotsoutsidebox, \
                                                    cornerkicks, \
                                                    offsides, \
                                                    ballpossession, \
                                                    totalpasses, \
                                                    passesaccurate, \
                                                    fouls, \
                                                    yellowcards, \
                                                    redcards, \
                                                    data_source, \
                                                    created_by)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
              returning id \
              """
        ds = 'API-Football'
        cb = 'gislobo'
        params = (
            databasefixtureid,
            databaseteamid,
            shotsongoal,
            shotsoffgoal,
            totalshots,
            blockedshots,
            goalkeepersaves,
            shotsinsidebox,
            shotsoutsidebox,
            cornerkicks,
            offsides,
            ballpossession,
            totalpasses,
            passesaccurate,
            fouls,
            yellowcards,
            redcards,
            ds,
            cb,
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                newid = cur.fetchone()[0]
                print(f"New fixturestatistcs id: {newid}")

        print("")
        print("---------------------------------")


def playerstatisticsfunction(payload, f, conn):
    ## Grab the database fixtureid
    print("Grabbing the database fixture id...")
    with conn.cursor() as cur:
        cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (f,))
        existingfixtures = cur.fetchall()
    existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if
                            existingfixture[0] is not None}
    dbfixtureid = None
    if f in existingfixturesdict:
        dbfixtureid = existingfixturesdict[f]
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
        return
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
        print(
            "The number of events in the response matches the number of events the API initially tells us there are.  Proceeding.")
        print("")
    else:
        print(
            "Something is wrong, the number of events in the response doesn't match the number of events the API tells us there are.")
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
        existingteamsdict = {existingteam[0]: existingteam[1] for existingteam in existingteams if
                             existingteam[0] is not None}
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
            existingplayersdict = {existingplayer[0]: existingplayer[1] for existingplayer in existingplayers if
                                   existingplayer[0] is not None}
            dbplayerid = None
            if apiplayerid in existingplayersdict:
                dbplayerid = existingplayersdict[apiplayerid]
                print(f"The database player id is {dbplayerid}.")
                print("")
            else:
                print(f"API Player ID {apiplayerid} is not in your database.")
                dbplayerid = int(input("Enter the database player id for this player:  "))

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
            existingpositionsdict = {existingposition[0]: existingposition[1] for existingposition in existingpositions
                                     if existingposition[0] is not None}
            if apiposition in existingpositionsdict:
                positionid = existingpositionsdict[apiposition]
                print(f"The position already exists in the database, position id:  {positionid}.")
            else:
                print(f"The position does not exist in the database, inserting it...")
                ds = 'API-Football'
                cb = 'gislobo'
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("insert into public.position (position, data_source, created_by) values (%s, %s, %s) returning id", (apiposition, ds, cb,))
                        positionid = cur.fetchone()[0]
                        print(f"...position inserted, position id:  {positionid}.")

            # Get the player's rating, turn it from a string to numeric
            print("Getting the player's rating, and turning it from a string to numeric...")
            ratingstr = games.get("rating")
            print(f"Api rating:  {ratingstr}.")
            if ratingstr in ['-', '–', None, ''] or (
                    isinstance(ratingstr, str) and ratingstr.strip() in ['-', '–', '']):
                print("This player has no rating, keeping rating as None.")
                pass  # keeps rating as None
            else:
                try:
                    rating = float(ratingstr)
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert rating '{ratingstr}' to float. Keeping as None.")
                    rating = None
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
                  insert into public.fixtureplayerstatistics (dbfixtureid, \
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
                                                              penaltiessaved, \
                                                              data_source, \
                                                              created_by)
                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                  returning id \
                  """
            ds = 'API-Football'
            cb = 'gislobo'
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
                ds,
                cb,
            )

            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    newid = cur.fetchone()[0]
                    print(f"...insert successful, new id:  {newid}.")

            print("")
            print("---------------------------------")
            print("")


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
    insert into public.coach (apifootballid, firstname, lastname, birthdate, birthplace, birthcountrycode, nationality, data_source, created_by)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning id
    """
    ds = 'API-Football'
    cb = 'gislobo'
    params = (
        aid,
        firstname,
        lastname,
        birthdate,
        birthplace,
        birthcountrycode,
        nationality,
        ds,
        cb,
    )

    with c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            newid = cur.fetchone()[0]
            print(f"New coach id: {newid}.")

    return newid


def lineupsfunction(payload, f, conn, headers, apiconn):
    ## Grab the database fixtureid
    print("Grabbing the database fixture id...")
    with conn.cursor() as cur:
        cur.execute("SELECT apisportsid, id from public.fixture where apisportsid = %s", (f,))
        existingfixtures = cur.fetchall()
    existingfixturesdict = {existingfixture[0]: existingfixture[1] for existingfixture in existingfixtures if
                            existingfixture[0] is not None}
    fixtureid = None
    if f in existingfixturesdict:
        fixtureid = existingfixturesdict[f]
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
        return
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
        existingteamsdict = {existingteam[0]: existingteam[1] for existingteam in existingteams if
                             existingteam[0] is not None}
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
        existingcoachesdict = {existingcoach[0]: existingcoach[1] for existingcoach in existingcoaches if
                               existingcoach[0] is not None}
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
        existingformationsdict = {existingformation[0]: existingformation[1] for existingformation in existingformations
                                  if existingformation[0] is not None}
        formationid = None
        if formation in existingformationsdict:
            formationid = existingformationsdict[formation]
            print(f"The database formation id is {formationid}.")
            print("")
        else:
            print(f"Formation {formation} is not in your database.")
            print("Adding formation to database...")
            ds = 'API-Football'
            cb = 'gislobo'
            with conn:
                with conn.cursor() as cur:
                    cur.execute("insert into public.formation (formation, data_source, created_by) values (%s, %s, %s) returning id", (formation, ds, cb,))
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
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
                    databaseplayerid = cur.fetchone()[0]
                print(f"The database player id is {databaseplayerid}.")
            except TypeError:
                print(f"The player doesn't exist in the database, setting playerid to 707 (null).")
                databaseplayerid = 707

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
        substitute8 = None
        substitute9 = None
        substitute10 = None
        substitute11 = None
        substitute12 = None

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
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id from public.player WHERE apifootballid = %s", (apiplayerid,))
                    databaseplayerid = cur.fetchone()[0]
                print(f"The database player id is {databaseplayerid}.")
            except TypeError:
                print(f"The player doesn't exist in the database, setting playerid to 707 (null).")
                databaseplayerid = 707

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
            elif count2 == 8:
                substitute8 = databaseplayerid
                print(f"Substitute 8 is {substitute8}.")
            elif count2 == 9:
                substitute9 = databaseplayerid
                print(f"Substitute 9 is {substitute9}.")
            elif count2 == 10:
                substitute10 = databaseplayerid
                print(f"Substitute 10 is {substitute10}.")
            elif count2 == 11:
                substitute11 = databaseplayerid
                print(f"Substitute 11 is {substitute11}.")
            elif count2 == 12:
                substitute12 = databaseplayerid
                print(f"Substitute 12 is {substitute12}.")
            else:
                print("Something is wrong, the number of substitutes is more than 12.")
                sys.exit(0)

        # Inserting variables into database
        print("Inserting variables into database...")
        sql = """
              insert into public.fixturelineups (fixtureid, \
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
                                                 substitute7, \
                                                 substitute8, \
                                                 substitute9, \
                                                 substitute10, \
                                                 substitute11, \
                                                 substitute12, \
                                                 data_source, \
                                                 created_by)
              values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
              returning id \
              """
        ds = 'API-Football'
        cb = 'gislobo'
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
            substitute8,
            substitute9,
            substitute10,
            substitute11,
            substitute12,
            ds,
            cb,
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                newid = cur.fetchone()[0]
                print(f"...insert successful, new id: {newid}.")

        print("")
        print("---------------------------------")
        print("")




def main():
    # list out fixtures
    ## 2017 fixture list
    # fixturelist = [147926, 147936, 147940, 147953, 147967, 147976, 147992, 148006, 148019, 148029, 148043, 148057,
    #                148066, 148074, 148078, 148088, 148096, 280488, 280464, 148109, 148114, 148131, 148143, 148164,
    #                148186, 148194, 148217, 148219, 148221, 148232, 148243, 148245, 148255, 148264, 148271, 148281,
    #                147915]
    ## 2018 !!!MLS!!! fixture list
    # fixturelist = [147510, 147526, 147533, 147548, 147555, 147573, 147582, 147586, 147604, 147608, 147621, 147633,
    #                147646, 147656, 147662, 147672, 147686, 147690, 147701, 147705, 147727, 147730, 147745, 147755,
    #                147788, 147794, 147816, 147822, 147835, 147838, 147857, 147860, 147882, 147894]
    ## 2018 NON-MLS fixture list [280386, 280371, 147501, 147502, 147493, 147494, 147492]
    ## 2019 MLS regular season and MLS playoffs fixture list]
    # fixturelist = [128178, 128189, 128200, 128213, 128237, 128251, 128266, 128286, 128287, 128301, 128305, 128316,
    #                128321, 128332, 128339, 128364, 128370, 128381, 128396, 128408, 128410, 128424, 128429, 128440,
    #                128461, 128481, 128486, 128502, 128521, 128531, 128534, 128546, 128555, 128564, 246444, 250195,
    #                250198]
    ## 2020 MLS regular season and MLS is back tournament fixture list
    # fixturelist = [634454, 634443, 634431, 634415, 634407, 634394, 634376, 634363, 634349, 628454, 628438, 628427,
    #                588656, 588611, 588644, 588627, 588616, 588600, 566056, 566043, 566031, 564278, 564266]
    ## 2021 MLS regular season and the one MLS playoff game
    fixturelist = [684131, 684144, 695168, 695187, 695199, 695219, 695227, 695246, 695249, 695269, 695278, 695296,
                   695297, 695311, 695323, 695340, 695355, 695366, 695387, 695393, 695410, 695419, 695437, 695452,
                   695463, 695476, 695490, 695505, 695521, 695529, 695554, 695567, 695578, 695582, 812188]




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

    # Connect
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
    )

    for fixture in fixturelist:
        print(f"\n{'=' * 100}")
        print(f"{' ' * 10}Running {fixture}...")
        print(f"{'=' * 100}\n")

        print("Getting fixture data from api...")
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        path = f"/fixtures?id={fixture}"
        apiconn.request("GET", path, headers=headers)
        res = apiconn.getresponse()
        raw = res.read()
        apiconn.close()
        payload = json.loads(raw.decode("utf-8"))
        print(f"Fixture payload data:  {payload}.")

        print(f"\n{'=' * 50}")
        print("Players...")
        print(f"{'=' * 50}\n")
        players(payload, headers, conn)
        print(f"\n{'=' * 50}")
        print(f"...Players are done for {fixture}.")
        print(f"{'=' * 50}\n")

        print(f"\n{'=' * 50}")
        print("Fixture...")
        print(f"{'=' * 50}\n")
        fixturefunction(payload, fixture, headers, conn)
        print(f"\n{'=' * 50}")
        print(f"...Fixture is done for {fixture}.")
        print(f"{'=' * 50}\n")

        print(f"\n{'=' * 50}")
        print("Events...")
        print(f"{'=' * 50}\n")
        print("Getting events data from api...")
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        eventpath = f"/fixtures/events?fixture={fixture}"
        apiconn.request("GET", eventpath, headers=headers)
        eventres = apiconn.getresponse()
        eventraw = eventres.read()
        apiconn.close()
        eventpayload = json.loads(eventraw.decode("utf-8"))
        eventfunction(eventpayload, fixture, conn)
        print(f"\n{'=' * 50}")
        print(f"...Events are done for {fixture}.")
        print(f"{'=' * 50}\n")

        print(f"\n{'=' * 50}")
        print("Fixture Statistics...")
        print(f"{'=' * 50}\n")
        print("Getting Fixture Statistics data from api...")
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        statisticspath = f"/fixtures/statistics?fixture={fixture}"
        apiconn.request("GET", statisticspath, headers=headers)
        statisticsres = apiconn.getresponse()
        statisticsraw = statisticsres.read()
        apiconn.close()
        statisticspayload = json.loads(statisticsraw.decode("utf-8"))
        statisticsfunction(statisticspayload, fixture, conn)
        print(f"\n{'=' * 50}")
        print(f"...Fixture Statistics are done for {fixture}.")
        print(f"{'=' * 50}\n")

        print(f"\n{'=' * 50}")
        print("Player Statistics...")
        print(f"{'=' * 50}\n")
        print("Getting Player Statistics data from api...")
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        playerstatisticspath = f"/fixtures/players?fixture={fixture}"
        apiconn.request("GET", playerstatisticspath, headers=headers)
        playerstatisticsres = apiconn.getresponse()
        playerstatisticsraw = playerstatisticsres.read()
        apiconn.close()
        playerstatisticspayload = json.loads(playerstatisticsraw.decode("utf-8"))
        playerstatisticsfunction(playerstatisticspayload, fixture, conn)
        print(f"\n{'=' * 50}")
        print(f"...Player Statistics are done for {fixture}.")
        print(f"{'=' * 50}\n")

        print(f"\n{'=' * 50}")
        print("Lineups...")
        print(f"{'=' * 50}\n")
        print("Getting Lineups data from api...")
        apiconn = http.client.HTTPSConnection("v3.football.api-sports.io")
        lineupspath = f"/fixtures/lineups?fixture={fixture}"
        apiconn.request("GET", lineupspath, headers=headers)
        lineupsres = apiconn.getresponse()
        lineupsraw = lineupsres.read()
        apiconn.close()
        lineupspayload = json.loads(lineupsraw.decode("utf-8"))
        lineupsfunction(lineupspayload, fixture, conn, headers, apiconn)
        print(f"\n{'=' * 50}")
        print(f"...Lineups are done for {fixture}.")
        print(f"{'=' * 50}\n")



if __name__ == "__main__":
    main()