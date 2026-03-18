# 🌤️ Smart Weather Analytics Dashboard

A real-time weather data pipeline and interactive dashboard tracking **21 cities across India and Canada** with ML-based temperature forecasting.

**Live Demo:** [Click to view →](https://weather-analytics-dashboard-jp.streamlit.app)

---

## 🚀 Features

- **Real-time data pipeline** — fetches live weather every hour via OpenWeatherMap API
- **21 cities** — 11 Indian cities + 10 Canadian cities
- **11,000+ records** stored in cloud PostgreSQL (Neon)
- **Interactive dashboard** — temperature trends, humidity, wind speed, heatmaps
- **ML forecasting** — Linear Regression model predicts next 24h temperature per city
- **AWS S3 archival** — raw JSON snapshots stored for audit trail
- **Deployed** on Streamlit Cloud

---

## 🏙️ Cities Tracked

| 🇮🇳 India | 🇨🇦 Canada |
|-----------|------------|
| Mumbai | Toronto |
| Delhi | Vancouver |
| Bengaluru | Montreal |
| Chennai | Calgary |
| Kolkata | Ottawa |
| Hyderabad | Edmonton |
| Pune | Winnipeg |
| Ahmedabad | Quebec City |
| Jaipur | Halifax |
| Lucknow | Victoria |
| Vadodara | |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Data ingestion | Python · OpenWeatherMap API |
| Database | PostgreSQL (Neon cloud) |
| Cloud storage | AWS S3 |
| Automation | APScheduler |
| Dashboard | Streamlit · Plotly |
| ML Forecasting | Scikit-learn · Linear Regression |
| Deployment | Streamlit Cloud |

---

## 📁 Project Structure
```
weather-analytics/
├── app.py                  # Streamlit dashboard
├── requirements.txt
├── src/
│   ├── db_setup.py         # DB init + data seeding
│   ├── fetch_weather.py    # Live fetch → PostgreSQL + S3
│   ├── scheduler.py        # Hourly automation
│   └── forecast.py         # ML temperature forecasting
```

---

## ⚙️ Setup
```bash
git clone https://github.com/Jhil-Patel/weather-analytics-dashboard.git
cd weather-analytics-dashboard
pip install -r requirements.txt
cp .env.example .env        # add your API keys
python src/db_setup.py      # initialize DB
streamlit run app.py        # launch dashboard
```

---

## 🤖 ML Model

- **Algorithm:** Linear Regression per city
- **Features:** cyclic hour encoding (sin/cos), day of week, lag temperatures (3h, 6h, 24h ago), 24h rolling mean
- **Performance:** MAE < 2°C for Indian cities
