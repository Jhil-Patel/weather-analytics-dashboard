# 🌦️ Smart Weather Analytics Dashboard

Real-time weather data pipeline and interactive dashboard tracking 5 global cities.  
**Stack:** Python · PostgreSQL · Pandas · Plotly · Streamlit · AWS S3

---

## Architecture

```
OpenWeatherMap API
       │
       ▼
 fetch_weather.py  ──────────────────────┐
  (every 60 min)                         │
       │                                 ▼
       │                         AWS S3 (raw JSON archive)
       │                         s3://bucket/raw/YYYY/MM/DD/
       ▼
  PostgreSQL DB
  (weather table)
       │
       ▼
   app.py (Streamlit)
   └── Temperature trend chart
   └── Humidity bar chart
   └── Wind speed distribution
   └── Temp vs Humidity scatter
   └── Conditions breakdown
   └── Daily temperature heatmap
```

---

## Setup (Local)

### 1. Clone & install
```bash
git clone https://github.com/YOUR_USERNAME/weather-analytics.git
cd weather-analytics
pip install -r requirements.txt
```

### 2. Configure secrets
```bash
cp .env.example .env
# Edit .env with your keys (see below)
```

### 3. Get a free PostgreSQL database
- Go to **[neon.tech](https://neon.tech)** → Create project → Copy connection string
- Paste into `DATABASE_URL` in `.env`

### 4. Get OpenWeatherMap API key
- Register at [openweathermap.org](https://openweathermap.org/api) (free)
- Copy API key → paste into `OWM_API_KEY` in `.env`

### 5. (Optional) AWS S3 setup
- Create an S3 bucket (free tier: 5 GB)
- Run `aws configure` with your IAM credentials
- Set `AWS_S3_BUCKET` in `.env`

### 6. Initialize database & seed 10 000+ rows
```bash
python src/db_setup.py
```
This will:
- Create the `weather` table with indexes
- Fetch real forecast data (5-day × 5 cities)
- Fetch historical data via One Call API
- Backfill climatically realistic data to reach 10 000+ rows

### 7. Run the dashboard
```bash
streamlit run app.py
```

### 8. Start the automated scheduler
```bash
# In a separate terminal — fetches live data every 60 minutes
python src/scheduler.py
```

---

## Deploy to Streamlit Cloud (free)

1. Push your code to a **public GitHub repo**
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app → pick your repo
3. Set **Main file path** to `app.py`
4. Go to **Settings → Secrets** and paste:
   ```toml
   DATABASE_URL = "postgresql://user:password@host:5432/dbname"
   ```
5. Click **Deploy** — live in ~2 minutes ✅

---

## Database Schema

```sql
CREATE TABLE weather (
    id          SERIAL PRIMARY KEY,
    city        TEXT    NOT NULL,
    country     TEXT    NOT NULL,
    temperature REAL,             -- °C
    feels_like  REAL,             -- °C
    humidity    INTEGER,          -- %
    wind_speed  REAL,             -- m/s
    description TEXT,             -- e.g. "Scattered Clouds"
    timestamp   TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX idx_weather_city_ts ON weather(city, timestamp);
```

---

## S3 Archive Structure

Raw API responses are stored as:
```
s3://weather-analytics-raw/
└── raw/
    └── 2026/
        └── 03/
            └── 17/
                ├── weather_0900.json
                ├── weather_1000.json
                └── ...
```

Each file contains the full OpenWeatherMap JSON response for all 5 cities,  
making the archive queryable with AWS Athena or S3 Select.

---

## Cities Tracked

| City      | Country | Lat     | Lon      |
|-----------|---------|---------|----------|
| Toronto   | CA      | 43.70°N | 79.42°W  |
| Vancouver | CA      | 49.25°N | 123.12°W |
| Montreal  | CA      | 45.51°N | 73.59°W  |
| Mumbai    | IN      | 19.08°N | 72.88°E  |
| London    | GB      | 51.51°N | 0.13°W   |

---

## Project Structure

```
weather-analytics/
├── app.py                  # Streamlit dashboard
├── requirements.txt
├── .env.example            # Template for secrets
├── .gitignore
├── README.md
├── .streamlit/
│   └── secrets.toml        # For Streamlit Cloud (gitignored)
├── src/
│   ├── db_setup.py         # One-time DB init + 10k-row seed
│   ├── fetch_weather.py    # Live fetch → PostgreSQL + S3
│   └── scheduler.py        # Hourly automation via APScheduler
└── logs/
    └── scheduler.log       # Auto-created at runtime
```
