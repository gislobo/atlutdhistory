import os
import http.client
import json
import urllib.parse
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import sys

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
            "position": p.get("position")
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
    countryClean = normalizeName(country)
    print(f"Looking up {countryClean}...")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT LOWER(name) AS lname, code
            FROM public.country
            WHERE LOWER(name) = %s
            """,
            (countryClean,)
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
        sys.exit(0)
    else:
        print(f"Player {playerId} is not in the database, proceeding.")

print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

print("Getting Player ID...")
#playerId = int(input("Enter the Player ID:  "))
playerId = 50870
#playerId = 6068
print(f"You entered: {playerId}.")

# Connect once for lookups and load
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"],
)

playerLookup(conn, playerId)

print(f"Building the dicitonary for {playerId}...")
player = getPlayerProfile(headers, playerId)
print("...dictionary built.")
print("Replacing birthcountry and nationality with codes from database...")
#print(player.get(playerId).get("position"))
birthcountryname = player.get(playerId).get("birthcountrycode")
nationalityname = player.get(playerId).get("nationality")
positionname = player.get(playerId).get("position")
print(birthcountryname)
print(nationalityname)
print(positionname)

with conn:
    # Map birthcountry name to code in database and replace dict value
    print("Map birthcountry name to code in database and replace dict value...")
    birthCountryCodeMap = applyCountryCodes(conn, birthcountryname)
    birthCountryCode = next(iter(birthCountryCodeMap.values()))
    player[playerId]["birthcountrycode"] = birthCountryCode
    print("...done.")
    # Map nationality name to code in database and replace dict value
    print("Map nationality name to code in database and replace dict value...")
    nationalityCodeMap = applyCountryCodes(conn, nationalityname)
    nationalityCountryCode = next(iter(nationalityCodeMap.values()))
    player[playerId]["nationality"] = nationalityCountryCode
    print("...done.")

    positionId = getPositionId(conn, positionname)
    print(positionId)
    player[playerId]["position"] = positionId
    print(player)
