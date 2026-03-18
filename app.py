"""
app.py — Weather Analytics Dashboard
Run: streamlit run app.py
"""

import os
import datetime
import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Weather Analytics",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #f8fafc;
    color: #1e293b;
}
.main { background-color: #f8fafc; }
.block-container { padding: 2rem 2.5rem 1rem; }

.top-bar {
    background: linear-gradient(135deg, #1e40af 0%, #0369a1 50%, #0891b2 100%);
    border-radius: 16px;
    padding: 28px 36px;
    margin-bottom: 28px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.top-bar h1 { color: white; font-size: 26px; font-weight: 700; margin: 0; letter-spacing: -0.5px; }
.top-bar p  { color: rgba(255,255,255,0.75); font-size: 13px; margin: 6px 0 0; }
.top-bar-right { text-align: right; color: rgba(255,255,255,0.85); font-size: 12px; line-height: 1.9; }
.top-bar-right b { color: white; font-weight: 600; }

.city-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 4px;
    border-top: 4px solid var(--accent, #3b82f6);
}
.city-name { font-size: 12px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
.city-temp { font-size: 36px; font-weight: 700; color: #0f172a; line-height: 1; }
.city-temp span { font-size: 18px; color: #94a3b8; }
.city-meta { font-size: 12px; color: #94a3b8; margin-top: 8px; }
.city-desc { font-size: 12px; color: #475569; margin-top: 4px; font-weight: 500; }

.sec-header { font-size: 15px; font-weight: 600; color: #0f172a; margin: 8px 0 16px; padding-bottom: 10px; border-bottom: 2px solid #e2e8f0; }

.country-label { display: inline-block; background: #eff6ff; color: #1d4ed8; font-size: 11px; font-weight: 600; padding: 3px 12px; border-radius: 20px; margin-bottom: 12px; }

[data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #e2e8f0; }

.stat-row { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f1f5f9; font-size: 13px; color: #475569; }
.stat-row b { color: #1e40af; font-weight: 600; }

#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── DB ─────────────────────────────────────────────────────────────────────────
def get_db_url():
    try:
        return st.secrets["DATABASE_URL"]
    except Exception:
        return os.getenv("DATABASE_URL")

@st.cache_data(ttl=300)
def load_data():
    url = get_db_url()
    if not url:
        return pd.DataFrame()
    try:
        conn = psycopg2.connect(url.split("&channel_binding")[0], connect_timeout=10)
        df = pd.read_sql("SELECT * FROM weather ORDER BY timestamp DESC LIMIT 100000", conn)
        conn.close()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_count():
    url = get_db_url()
    if not url:
        return 0
    try:
        conn = psycopg2.connect(url.split("&channel_binding")[0], connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM weather")
        n = cur.fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0

df = load_data()
if df.empty:
    st.error("No data. Run `python src/db_setup.py` first.")
    st.stop()


# ── CITY CONFIG ────────────────────────────────────────────────────────────────
PALETTE = [
    "#3b82f6","#10b981","#f59e0b","#ef4444","#8b5cf6",
    "#06b6d4","#ec4899","#84cc16","#f97316","#6366f1",
    "#14b8a6","#e11d48","#0ea5e9","#a3e635","#fb923c",
    "#d946ef","#22d3ee","#4ade80","#facc15","#f43f5e",
]
all_cities = sorted(df["city"].unique().tolist())
CITY_COLORS = {city: PALETTE[i % len(PALETTE)] for i, city in enumerate(all_cities)}

INDIA_CITIES  = ["Mumbai","Delhi","Bengaluru","Chennai","Kolkata",
                 "Hyderabad","Pune","Ahmedabad","Jaipur","Lucknow","Vadodara"]
CANADA_CITIES = ["Toronto","Vancouver","Montreal","Calgary","Ottawa",
                 "Edmonton","Winnipeg","Quebec City","Halifax","Victoria"]


# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🌤️ Weather Analytics")
    st.markdown("<hr style='border:1px solid #e2e8f0;margin:8px 0 16px'>", unsafe_allow_html=True)

    st.markdown("**Filter by Country**")
    show_india  = st.checkbox("🇮🇳 India",  value=True)
    show_canada = st.checkbox("🇨🇦 Canada", value=True)

    available = [c for c in all_cities if
                 (show_india  and c in INDIA_CITIES) or
                 (show_canada and c in CANADA_CITIES) or
                 (c not in INDIA_CITIES and c not in CANADA_CITIES)]

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Select Cities**")
    selected = st.multiselect("Cities", options=available, default=available[:5],
                              label_visibility="collapsed")
    if not selected:
        selected = available[:5]

    st.markdown("<br>", unsafe_allow_html=True)
    days_back = st.slider("📅 Days of history", 1, 30, 7)

    st.markdown("<br>", unsafe_allow_html=True)
    metric = st.radio("📊 Primary metric",
                      ["temperature","humidity","wind_speed","feels_like"],
                      format_func=lambda x: {
                          "temperature": "Temperature (°C)",
                          "humidity":    "Humidity (%)",
                          "wind_speed":  "Wind Speed (m/s)",
                          "feels_like":  "Feels Like (°C)",
                      }[x])

    st.markdown("<hr style='border:1px solid #e2e8f0;margin:16px 0 12px'>", unsafe_allow_html=True)
    total = get_count()
    for label, val in [
        ("Total records",  f"{total:,}"),
        ("Cities tracked", str(df["city"].nunique())),
        ("Last updated",   datetime.datetime.now().strftime("%b %d, %H:%M")),
        ("Cache TTL",      "5 min"),
    ]:
        st.markdown(f"<div class='stat-row'>{label}<b>{val}</b></div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↺  Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── FILTER ─────────────────────────────────────────────────────────────────────
cutoff   = datetime.datetime.now() - datetime.timedelta(days=days_back)
filtered = df[df["city"].isin(selected) & (df["timestamp"] >= cutoff)].copy()
latest   = (df[df["city"].isin(selected)]
            .sort_values("timestamp")
            .groupby("city").last()
            .reset_index())


# ── HEADER ─────────────────────────────────────────────────────────────────────
last_ts = datetime.datetime.now().strftime("%d %b %Y, %H:%M")
st.markdown(f"""
<div class="top-bar">
  <div>
    <h1>🌤️ Weather Analytics Dashboard</h1>
    <p>Real-time pipeline · OpenWeatherMap API · PostgreSQL · AWS S3</p>
  </div>
  <div class="top-bar-right">
    <div>Total records &nbsp;<b>{total:,}</b></div>
    <div>Cities tracked &nbsp;<b>{df["city"].nunique()}</b></div>
    <div>Last updated &nbsp;<b>{last_ts}</b></div>
  </div>
</div>
""", unsafe_allow_html=True)


# ── CITY CARDS ─────────────────────────────────────────────────────────────────
def render_cards(rows, label):
    if rows.empty:
        return
    st.markdown(f"<div class='country-label'>{label}</div>", unsafe_allow_html=True)
    cols = st.columns(min(len(rows), 5))
    for i, (_, row) in enumerate(rows.iterrows()):
        color = CITY_COLORS.get(row["city"], "#3b82f6")
        temp  = row["temperature"]
        desc  = str(row["description"]).lower()
        icon  = "☀️" if temp > 28 else ("🌧️" if "rain" in desc else ("❄️" if temp < 5 else ("🌥️" if temp < 15 else "⛅")))
        with cols[i % 5]:
            st.markdown(f"""
            <div class="city-card" style="--accent:{color}">
                <div class="city-name">{row["city"]}</div>
                <div class="city-temp">{icon} {temp:.1f}<span>°C</span></div>
                <div class="city-desc">{row["description"]}</div>
                <div class="city-meta">💧 {row["humidity"]}%  ·  💨 {row["wind_speed"]} m/s</div>
            </div>
            """, unsafe_allow_html=True)

india_l  = latest[latest["city"].isin(INDIA_CITIES)]
canada_l = latest[latest["city"].isin(CANADA_CITIES)]
if not india_l.empty:  render_cards(india_l,  "🇮🇳 India")
if not canada_l.empty: render_cards(canada_l, "🇨🇦 Canada")

st.markdown("<br>", unsafe_allow_html=True)


# ── PLOT THEME ─────────────────────────────────────────────────────────────────
LP = dict(
    paper_bgcolor="white",
    plot_bgcolor="#f8fafc",
    font=dict(family="Inter", color="#475569", size=12),
)


# ── TREND CHART ────────────────────────────────────────────────────────────────
st.markdown(f"<div class='sec-header'>📈 {metric.replace('_',' ').title()} Trend — Last {days_back} Days</div>",
            unsafe_allow_html=True)

fig_trend = go.Figure()
for city in selected:
    cdf = (filtered[filtered["city"] == city]
           .sort_values("timestamp")
           .set_index("timestamp")
           .resample("3h")[metric].mean()
           .reset_index())
    fig_trend.add_trace(go.Scatter(
        x=cdf["timestamp"], y=cdf[metric], name=city, mode="lines",
        line=dict(color=CITY_COLORS.get(city, "#64748b"), width=2),
        hovertemplate=f"<b>{city}</b><br>%{{x|%b %d %H:%M}}<br>{metric}: %{{y:.1f}}<extra></extra>",
    ))
fig_trend.update_layout(
    **LP, height=340, hovermode="x unified",
    xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickfont=dict(color="#94a3b8")),
    yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0"),
    legend=dict(bgcolor="white", bordercolor="#e2e8f0", borderwidth=1),
    margin=dict(l=10, r=10, t=10, b=10),
)
st.plotly_chart(fig_trend, use_container_width=True)


# ── HUMIDITY + WIND ────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.markdown("<div class='sec-header'>💧 Average Humidity by City</div>", unsafe_allow_html=True)
    hum = (filtered.groupby("city")["humidity"].mean()
           .reset_index().sort_values("humidity", ascending=True))
    fig_hum = px.bar(hum, x="humidity", y="city", orientation="h",
                     color="city", color_discrete_map=CITY_COLORS,
                     text=hum["humidity"].round(1))
    fig_hum.update_traces(texttemplate="%{text}%", textposition="outside")
    fig_hum.update_layout(
        **LP, height=380, showlegend=False,
        xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Humidity %"),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title=""),
        margin=dict(l=10, r=60, t=10, b=10),
    )
    st.plotly_chart(fig_hum, use_container_width=True)

with c2:
    st.markdown("<div class='sec-header'>💨 Wind Speed Distribution</div>", unsafe_allow_html=True)
    fig_wind = px.violin(filtered, x="city", y="wind_speed",
                         color="city", color_discrete_map=CITY_COLORS, box=True)
    fig_wind.update_layout(
        **LP, height=380, showlegend=False,
        xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0",
                   tickangle=-35, tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Wind Speed (m/s)"),
        margin=dict(l=10, r=10, t=10, b=70),
    )
    st.plotly_chart(fig_wind, use_container_width=True)


# ── SCATTER + CONDITIONS ───────────────────────────────────────────────────────
c3, c4 = st.columns(2)

with c3:
    st.markdown("<div class='sec-header'>🌡️ Temperature vs Humidity</div>", unsafe_allow_html=True)
    sample = filtered.sample(min(3000, len(filtered)))
    fig_sc = px.scatter(sample, x="temperature", y="humidity",
                        color="city", color_discrete_map=CITY_COLORS,
                        opacity=0.5, hover_data=["city","description"])
    fig_sc.update_layout(
        **LP, height=340,
        xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Temperature (°C)"),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Humidity %"),
        legend=dict(bgcolor="white", bordercolor="#e2e8f0",
                    borderwidth=1, font=dict(color="#475569", size=10)),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_sc, use_container_width=True)

with c4:
    st.markdown("<div class='sec-header'>🌤️ Weather Conditions Breakdown</div>", unsafe_allow_html=True)
    cond = filtered.groupby(["city","description"]).size().reset_index(name="count")
    fig_cond = px.bar(cond, x="city", y="count", color="description",
                      barmode="stack",
                      color_discrete_sequence=px.colors.qualitative.Pastel)
    fig_cond.update_layout(
        **LP, height=340,
        xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0",
                   tickangle=-35, tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Records"),
        legend=dict(bgcolor="white", bordercolor="#e2e8f0", borderwidth=1,
                    font=dict(color="#475569", size=10)),
        margin=dict(l=10, r=10, t=10, b=70),
    )
    st.plotly_chart(fig_cond, use_container_width=True)


# ── HEATMAP ────────────────────────────────────────────────────────────────────
st.markdown("<div class='sec-header'>🗓️ Daily Average Temperature Heatmap</div>",
            unsafe_allow_html=True)

heat = (filtered.copy()
        .assign(date=lambda d: d["timestamp"].dt.strftime("%b %d"))
        .groupby(["city","date"])["temperature"].mean()
        .reset_index())

dates       = sorted(filtered["timestamp"].dt.strftime("%b %d").unique())
cities_heat = [c for c in selected if c in heat["city"].values]
z, ytxt     = [], []
for city in cities_heat:
    rd   = heat[heat["city"] == city].set_index("date")["temperature"]
    vals = [round(float(rd.get(d, 0)), 1) for d in dates]
    z.append(vals)
    ytxt.append(city)

fig_hm = go.Figure(go.Heatmap(
    z=z, x=dates, y=ytxt,
    text=[[f"{v}°" for v in row] for row in z],
    texttemplate="%{text}",
    colorscale="RdYlBu_r",
    colorbar=dict(title="°C", tickfont=dict(color="#475569", size=11)),
))
fig_hm.update_layout(
    paper_bgcolor="white", plot_bgcolor="white",
    font=dict(family="Inter", color="#475569", size=11),
    xaxis=dict(tickangle=-45, tickfont=dict(color="#94a3b8", size=10)),
    yaxis=dict(tickfont=dict(color="#475569")),
    height=max(300, len(cities_heat) * 36 + 80),
    margin=dict(l=10, r=10, t=10, b=60),
)
st.plotly_chart(fig_hm, use_container_width=True)


# ── RAW DATA ───────────────────────────────────────────────────────────────────
with st.expander("📋 Raw Data Table"):
    show = filtered[["timestamp","city","temperature","feels_like",
                     "humidity","wind_speed","description"]].copy()
    show["timestamp"] = show["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    show.columns = ["Time","City","Temp °C","Feels Like","Humidity %","Wind m/s","Condition"]
    st.dataframe(show.head(500), use_container_width=True)
    st.download_button("⬇️ Download CSV",
                       show.to_csv(index=False).encode("utf-8"),
                       "weather_data.csv", "text/csv")


# ── ML FORECAST ────────────────────────────────────────────────────────────────
st.markdown("<div class='sec-header'>🤖 ML Temperature Forecast — Next 24 Hours</div>",
            unsafe_allow_html=True)

st.markdown("""
<div style='background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;
            padding:14px 18px;margin-bottom:20px;font-size:13px;color:#1e40af;'>
    <b>How it works:</b> A Linear Regression model is trained on historical temperature
    patterns for each city using time-of-day cycles, day-of-week, and lag features
    (3h, 6h, 24h ago). It then predicts the next 24 hours of temperature.
</div>
""", unsafe_allow_html=True)

@st.cache_data(ttl=1800)
def get_forecasts(city_list):
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
    from forecast import run_all_forecasts
    city_df = df[df["city"].isin(city_list)].copy()
    return run_all_forecasts(city_df, hours_ahead=24)

with st.spinner("Training models and generating forecasts..."):
    forecasts, metrics_list = get_forecasts(tuple(sorted(selected)))

if not forecasts:
    st.warning("Not enough historical data to forecast. Run the scheduler for a few hours first.")
else:
    fig_fc = go.Figure()

    for city in selected:
        if city not in forecasts:
            continue
        # Last 48h actual data
        recent = (df[df["city"] == city]
                  .sort_values("timestamp")
                  .tail(48))
        if not recent.empty:
            fig_fc.add_trace(go.Scatter(
                x=recent["timestamp"], y=recent["temperature"],
                name=f"{city} (actual)", mode="lines",
                line=dict(color=CITY_COLORS.get(city, "#64748b"), width=2),
                hovertemplate=f"<b>{city}</b> actual<br>%{{x|%b %d %H:%M}}<br>%{{y:.1f}}°C<extra></extra>",
            ))
        # Forecast — dashed
        fdf = forecasts[city]
        fig_fc.add_trace(go.Scatter(
            x=fdf["timestamp"], y=fdf["predicted_temp"],
            name=f"{city} (forecast)", mode="lines",
            line=dict(color=CITY_COLORS.get(city, "#64748b"), width=2, dash="dash"),
            hovertemplate=f"<b>{city}</b> forecast<br>%{{x|%b %d %H:%M}}<br>%{{y:.1f}}°C<extra></extra>",
        ))

    fig_fc.update_layout(
        **LP, height=400, hovermode="x unified",
        xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", title="Temperature (°C)"),
        legend=dict(bgcolor="white", bordercolor="#e2e8f0", borderwidth=1,
                    font=dict(color="#475569", size=10)),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_fc, use_container_width=True)

    # Model metrics table
    st.markdown("<div class='sec-header'>📊 Model Performance Metrics</div>",
                unsafe_allow_html=True)
    st.markdown("""
    <div style='font-size:12px;color:#64748b;margin-bottom:12px;'>
    <b>MAE</b> = Mean Absolute Error in °C (lower is better) &nbsp;·&nbsp;
    <b>R²</b> = model fit score (1.0 = perfect) &nbsp;·&nbsp;
    <b>Training Records</b> = data points used to train the model
    </div>
    """, unsafe_allow_html=True)

    metrics_df = pd.DataFrame(metrics_list).sort_values("mae")
    metrics_df.columns = ["City", "MAE (°C)", "R² Score", "Training Records"]
    metrics_df["MAE (°C)"] = metrics_df["MAE (°C)"].apply(lambda x: f"{x:.2f}")
    metrics_df["R² Score"] = metrics_df["R² Score"].apply(lambda x: f"{x:.3f}")
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)


# ── FOOTER ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center;padding:24px 0 8px;font-size:12px;color:#94a3b8;'>
    Built with Python · Streamlit · PostgreSQL · Plotly · Scikit-learn · OpenWeatherMap API
</div>
""", unsafe_allow_html=True)