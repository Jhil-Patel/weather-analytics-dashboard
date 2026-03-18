"""
fetch_weather.py
----------------
Fetches current weather for all 5 cities from OpenWeatherMap API.
Stores records in PostgreSQL AND uploads a raw JSON snapshot to AWS S3.

Run manually:   python src/fetch_weather.py
Auto-scheduled: python src/scheduler.py  (runs this every hour)
"""

import os
import json
import datetime
import requests
import psycopg2
import psycopg2.extras
import boto3
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("OWM_API_KEY")
DB_URL     = os.getenv("DATABASE_URL")
S3_BUCKET  = os.getenv("AWS_S3_BUCKET")       # e.g. "weather-analytics-raw"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

CITIES = [
    # India
    {"name": "Mumbai",      "lat": 19.0760, "lon":  72.8777, "country": "IN"},
    {"name": "Delhi",       "lat": 28.6139, "lon":  77.2090, "country": "IN"},
    {"name": "Bengaluru",   "lat": 12.9716, "lon":  77.5946, "country": "IN"},
    {"name": "Chennai",     "lat": 13.0827, "lon":  80.2707, "country": "IN"},
    {"name": "Kolkata",     "lat": 22.5726, "lon":  88.3639, "country": "IN"},
    {"name": "Hyderabad",   "lat": 17.3850, "lon":  78.4867, "country": "IN"},
    {"name": "Pune",        "lat": 18.5204, "lon":  73.8567, "country": "IN"},
    {"name": "Ahmedabad",   "lat": 23.0225, "lon":  72.5714, "country": "IN"},
    {"name": "Jaipur",      "lat": 26.9124, "lon":  75.7873, "country": "IN"},
    {"name": "Lucknow",     "lat": 26.8467, "lon":  80.9462, "country": "IN"},
    {"name": "Vadodara",    "lat": 22.3072, "lon":  73.1812, "country": "IN"},
    # Canada
    {"name": "Toronto",     "lat": 43.7001, "lon": -79.4163, "country": "CA"},
    {"name": "Vancouver",   "lat": 49.2497, "lon":-123.1193, "country": "CA"},
    {"name": "Montreal",    "lat": 45.5088, "lon": -73.5878, "country": "CA"},
    {"name": "Calgary",     "lat": 51.0447, "lon": -114.0719,"country": "CA"},
    {"name": "Ottawa",      "lat": 45.4215, "lon": -75.6972, "country": "CA"},
    {"name": "Edmonton",    "lat": 53.5461, "lon": -113.4938,"country": "CA"},
    {"name": "Winnipeg",    "lat": 49.8951, "lon": -97.1384, "country": "CA"},
    {"name": "Quebec City", "lat": 46.8139, "lon": -71.2080, "country": "CA"},
    {"name": "Halifax",     "lat": 44.6488, "lon": -63.5752, "country": "CA"},
    {"name": "Victoria",    "lat": 48.4284, "lon":-123.3656, "country": "CA"},
]

INSERT_SQL = """
INSERT INTO weather (city, country, temperature, feels_like, humidity, wind_speed, description, timestamp)
VALUES (%(city)s, %(country)s, %(temperature)s, %(feels_like)s, %(humidity)s, %(wind_speed)s, %(description)s, %(timestamp)s)
ON CONFLICT (city, timestamp) DO NOTHING
"""


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────
def fetch_city(city: dict) -> dict | None:
    """Fetch current weather for one city. Returns structured dict or None."""
    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": API_KEY,
        "units": "metric",
    }
    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        raw  = resp.json()
        return {
            "city":        city["name"],
            "country":     city["country"],
            "temperature": raw["main"]["temp"],
            "feels_like":  raw["main"]["feels_like"],
            "humidity":    raw["main"]["humidity"],
            "wind_speed":  raw["wind"]["speed"],
            "description": raw["weather"][0]["description"].title(),
            "timestamp":   datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None, second=0, microsecond=0),
            "_raw":        raw,   # keep full API response for S3 archive
        }
    except Exception as exc:
        print(f"  ✗ {city['name']}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# POSTGRES
# ─────────────────────────────────────────────────────────────────────────────
def save_to_postgres(records: list[dict]):
    """Bulk-insert weather records into PostgreSQL."""
    # Strip the _raw field before inserting
    clean = [{k: v for k, v in r.items() if k != "_raw"} for r in records]
    conn  = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, clean, page_size=100)
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# AWS S3  — raw JSON archive
# ─────────────────────────────────────────────────────────────────────────────
def upload_to_s3(records: list[dict], run_ts: datetime.datetime):
    """
    Uploads the raw API responses as a timestamped JSON file to S3.
    Key pattern:  raw/<YYYY>/<MM>/<DD>/weather_<HH><MM>.json
    This creates a queryable archive in S3 / Athena-compatible structure.
    """
    if not S3_BUCKET:
        print("  ⚠  AWS_S3_BUCKET not set — skipping S3 upload.")
        return

    payload = {
        "run_utc":  run_ts.isoformat(),
        "cities":   len(records),
        "records":  [r.get("_raw", r) for r in records],
    }
    key = (
        f"raw/{run_ts.year:04d}/{run_ts.month:02d}/{run_ts.day:02d}/"
        f"weather_{run_ts.hour:02d}{run_ts.minute:02d}.json"
    )

    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = json.dumps(payload, default=str).encode("utf-8"),
            ContentType = "application/json",
        )
        print(f"  ✓ S3 upload → s3://{S3_BUCKET}/{key}")
    except Exception as exc:
        print(f"  ✗ S3 upload failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run_fetch():
    run_ts  = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    label   = run_ts.strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'─'*50}\n[{label}] Fetching weather data…")

    records = []
    for city in CITIES:
        record = fetch_city(city)
        if record:
            print(f"  ✓ {city['name']:12s}  {record['temperature']:5.1f}°C  "
                  f"{record['humidity']}%  {record['description']}")
            records.append(record)

    if records:
        save_to_postgres(records)
        print(f"  → Saved {len(records)} rows to PostgreSQL.")
        upload_to_s3(records, run_ts)
    else:
        print("  ✗ No records fetched.")

    print(f"  Done ({len(records)}/{len(CITIES)} cities).")
    return len(records)


if __name__ == "__main__":
    run_fetch()