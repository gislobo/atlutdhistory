import os
import http.client
import json
import urllib.parse
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

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


def fetchPlayerProfile(headers, playerId):
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


print("Loading headers...")
headers = loadHeaders("headers.json")
print("...headers loaded.")
print("Loading DB config...")
db = loadDbConfig("dbConfig.json")
print("...DB config loaded.")

print("Getting Player ID...")
#playerId = int(input("Enter the Player ID:  "))
playerId = 6068
print(f"You entered: {playerId}.")
player = fetchPlayerProfile(headers, playerId)
print(player.get(playerId).get("position"))