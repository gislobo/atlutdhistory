import os
import http.client
import json
import urllib.parse
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

API_HOST = "v3.football.api-sports.io"


def parse_height_cm(height_str):
    if not height_str:
        return None
    try:
        return int(height_str.split()[0])
    except Exception:
        return None


def parse_weight_kg(weight_str):
    if not weight_str:
        return None
    try:
        return int(weight_str.split()[0])
    except Exception:
        return None


def parse_birth_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None


def load_headers(headers_path="headers.json"):
    with open(headers_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_db_config(config_path="dbConfig.json"):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "host": cfg.get("host", "localhost"),
        "port": int(cfg.get("port", 5432)),
        "database": cfg.get("database", "atlutdhistory"),
        "user": cfg.get("user", "your_user"),
        "password": cfg.get("password", "your_password"),
        "schema": cfg.get("schema", "public"),
        "table": cfg.get("table", "player"),
    }


def extract_position_name(stats):
    """
    stats: list of statistics objects from the API
    Returns the first non-empty position found in any stats[i].games.position (fallback to stats[i].position if present).
    """
    if not stats:
        return None
    for s in stats:
        if not s:
            continue
        games = (s.get("games") or {})
        pos = games.get("position") or s.get("position")
        if pos and str(pos).strip():
            return pos
    return None


def fetch_player_profiles(headers, player_ids):
    """
    player_ids: list[int]
    Returns: list of dicts with raw country names and raw position name for later lookups.
    """
    conn = http.client.HTTPSConnection(API_HOST)
    normalized = []

    for pid in player_ids:
        qs = urllib.parse.urlencode({"player": pid})
        path = f"/players/profiles?{qs}"

        conn.request("GET", path, headers=headers)
        res = conn.getresponse()
        raw = res.read()

        if res.status != 200:
            raise RuntimeError(f"API error {res.status}: {raw[:200]!r}")

        payload = json.loads(raw.decode("utf-8"))

        for item in payload.get("response", []):
            p = item.get("player", {}) or {}
            birth = p.get("birth", {}) or {}
            stats = item.get("statistics") or []
            # CHANGED: find the first available position across all statistics entries
            position_name = extract_position_name(stats)

            normalized.append(
                {
                    "apifootballid": p.get("id"),
                    "firstname": p.get("firstname"),
                    "lastname": p.get("lastname"),
                    "birthdate": parse_birth_date(birth.get("date")),
                    "birthplace": birth.get("place"),
                    # Raw names for mapping
                    "birth_country_name": (birth.get("country") or None),
                    "nationality_name": (p.get("nationality") or None),
                    "position_name": position_name,
                    # Parsed numeric attributes
                    "heightcm": parse_height_cm(p.get("height")),
                    "weightkg": parse_weight_kg(p.get("weight")),
                }
            )

    conn.close()
    return normalized


def map_country_names_to_codes(pg_conn, names):
    """
    names: set[str] of country names (case-insensitive)
    Returns dict: lower(name) -> code from public.country
    """
    if not names:
        return {}
    clean = sorted({n.strip().lower() for n in names if n and n.strip()})
    if not clean:
        return {}

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT LOWER(name) AS lname, code
            FROM public.country
            WHERE LOWER(name) = ANY(%s)
            """,
            (clean,),
        )
        rows = cur.fetchall()
    return {lname: code for lname, code in rows}


def apply_country_codes(pg_conn, players):
    """
    Mutates players list: sets birthcountrycode and nationality (as codes)
    from public.country using name lookups.
    """
    name_pool = set()
    for r in players:
        if r.get("birth_country_name"):
            name_pool.add(r["birth_country_name"])
        if r.get("nationality_name"):
            name_pool.add(r["nationality_name"])

    code_map = map_country_names_to_codes(pg_conn, name_pool)

    for r in players:
        birth_code = None
        nat_code = None
        if r.get("birth_country_name"):
            birth_code = code_map.get(r["birth_country_name"].strip().lower())
        if r.get("nationality_name"):
            nat_code = code_map.get(r["nationality_name"].strip().lower())

        r["birthcountrycode"] = birth_code
        r["nationality"] = nat_code

        # Drop helper fields
        r.pop("birth_country_name", None)
        r.pop("nationality_name", None)


def load_existing_positions(pg_conn, names_lower):
    """
    names_lower: list[str] lower-cased position names
    Returns dict lower(name) -> id from public.position
    """
    if not names_lower:
        return {}
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT LOWER(position) AS lname, id
            FROM public.position
            WHERE LOWER(position) = ANY(%s)
            """,
            (names_lower,),
        )
        return {lname: pid for lname, pid in cur.fetchall()}


def insert_missing_positions(pg_conn, missing_names_lower_to_original):
    """
    missing_names_lower_to_original: dict lower(name) -> original_name
    Inserts any missing positions with generated ids (max(id)+1, +1, ...).
    Returns dict lower(name) -> id for the inserted ones.
    """
    if not missing_names_lower_to_original:
        return {}

    with pg_conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM public.position")
        start_id = cur.fetchone()[0]
        inserted = {}
        # Deterministic ordering to avoid gaps across runs
        ordered = sorted(missing_names_lower_to_original.items(), key=lambda x: x[0])
        values = []
        for i, (lname, original) in enumerate(ordered, start=1):
            new_id = start_id + i
            inserted[lname] = new_id
            values.append((new_id, original))

        execute_values(
            cur,
            "INSERT INTO public.position (id, position) VALUES %s",
            values,
        )
        return inserted


def apply_position_ids(pg_conn, players):
    """
    Mutates players list: sets 'position' to the integer id from public.position.
    If the position name doesn't exist, inserts it and uses the new id.
    """
    # Collect unique position names (non-empty)
    names = {r["position_name"] for r in players if r.get("position_name")}
    if not names:
        for r in players:
            r["position"] = None
        return

    names_lower = sorted({n.strip().lower() for n in names if n and n.strip()})
    existing_map = load_existing_positions(pg_conn, names_lower)

    # Determine which are missing
    missing_lower_to_original = {}
    original_by_lower = {}
    for n in names:
        lname = n.strip().lower()
        original_by_lower[lname] = n
        if lname not in existing_map:
            missing_lower_to_original[lname] = n

    inserted_map = insert_missing_positions(pg_conn, missing_lower_to_original)

    # Merge maps
    id_map = {**existing_map, **inserted_map}

    # Apply to players
    for r in players:
        pname = r.get("position_name")
        if pname and pname.strip().lower() in id_map:
            r["position"] = id_map[pname.strip().lower()]
        else:
            r["position"] = None
        # Drop helper field
        r.pop("position_name", None)


def upsert_players(pg_conn, schema, table, rows):
    """
    Manual upsert by apifootballid: update, then insert missing.
    Target: schema.table with columns:
      id, apifootballid, firstname, lastname, birthdate, birthplace,
      birthcountrycode, nationality, heightcm, weightkg, position
    """
    if not rows:
        return

    fqn = f"{schema}.{table}"

    with pg_conn.cursor() as cur:
        # Update pass
        for r in rows:
            cur.execute(
                f"""
                UPDATE {fqn}
                SET firstname = %s,
                    lastname = %s,
                    birthdate = %s,
                    birthplace = %s,
                    birthcountrycode = %s,
                    nationality = %s,
                    heightcm = %s,
                    weightkg = %s,
                    position = %s
                WHERE apifootballid = %s
                """,
                (
                    r["firstname"],
                    r["lastname"],
                    r["birthdate"],
                    r["birthplace"],
                    r["birthcountrycode"],
                    r["nationality"],
                    r["heightcm"],
                    r["weightkg"],
                    r["position"],
                    r["apifootballid"],
                ),
            )

        # Collect missing via existence check
        missing = []
        for r in rows:
            cur.execute(
                f"SELECT 1 FROM {fqn} WHERE apifootballid = %s",
                (r["apifootballid"],),
            )
            if cur.fetchone() is None:
                missing.append(r)

        # Insert missing
        if missing:
            execute_values(
                cur,
                f"""
                INSERT INTO {fqn} (
                    apifootballid, firstname, lastname, birthdate, birthplace,
                    birthcountrycode, nationality, heightcm, weightkg, position
                )
                VALUES %s
                """,
                [
                    (
                        r["apifootballid"],
                        r["firstname"],
                        r["lastname"],
                        r["birthdate"],
                        r["birthplace"],
                        r["birthcountrycode"],
                        r["nationality"],
                        r["heightcm"],
                        r["weightkg"],
                        r["position"],
                    )
                    for r in missing
                ],
            )


def main():
    headers = load_headers("../headers.json")
    db = load_db_config("../dbConfig.json")

    # PLAYER_IDS env var like "6068,1234,5678" or fallback to a single example
    player_ids_env = os.getenv("PLAYER_IDS")
    if player_ids_env:
        player_ids = [int(x.strip()) for x in player_ids_env.split(",") if x.strip()]
    else:
        player_ids = [6068]

    # Fetch API data (with raw country and position names)
    players = fetch_player_profiles(headers, player_ids)

    # Connect once for lookups and load
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["database"],
        user=db["user"],
        password=db["password"],
    )
    try:
        with conn:
            # Map country names -> codes and apply to players
            apply_country_codes(conn, players)
            # Map/insert positions -> ids and apply to players
            apply_position_ids(conn, players)
            # Upsert players
            upsert_players(conn, db["schema"], db["table"], players)

        print(f"Loaded/updated {len(players)} player record(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()