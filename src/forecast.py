"""
forecast.py
-----------
Predicts next 24 hours of temperature for each city using
a Linear Regression model trained on historical patterns.

Features used:
  - hour_sin / hour_cos  (cyclic encoding of hour — captures diurnal cycle)
  - day of week          (captures weekly patterns)
  - lag_3h               (temperature 3 hours ago)
  - lag_6h               (temperature 6 hours ago)
  - lag_24h              (same hour yesterday)
  - rolling_mean_24h     (24-hour moving average)

Intentionally simple and explainable — perfect for interviews.
"""

import datetime
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score

FEATURE_COLS = ["hour_sin", "hour_cos", "dayofweek",
                "lag_1", "lag_2", "lag_8", "roll_8"]


def build_features(df_city: pd.DataFrame) -> pd.DataFrame:
    df = (df_city.copy()
          .sort_values("timestamp")
          .reset_index(drop=True)
          .rename(columns={"temperature": "temp"}))

    # Use whatever interval the data has — don't force hourly resample
    df = df[["timestamp", "temp"]].drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)

    df["hour"]      = df["timestamp"].dt.hour
    df["dayofweek"] = df["timestamp"].dt.dayofweek
    df["hour_sin"]  = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]  = np.cos(2 * np.pi * df["hour"] / 24)

    # Use smaller lags that work with 3h-interval data
    df["lag_1"]     = df["temp"].shift(1)   # 1 reading ago
    df["lag_2"]     = df["temp"].shift(2)   # 2 readings ago
    df["lag_8"]     = df["temp"].shift(8)   # ~24h ago at 3h intervals
    df["roll_8"]    = df["temp"].shift(1).rolling(8).mean()  # rolling avg

    return df.dropna().reset_index(drop=True)

def train_and_forecast(df_city: pd.DataFrame, city: str, hours_ahead: int = 24):
    feat_df = build_features(df_city)
    if len(feat_df) < 10:
        return None, None, None

    X = feat_df[FEATURE_COLS].values
    y = feat_df["temp"].values

    split   = max(len(X) - 24, int(len(X) * 0.85))
    X_tr, X_te = X[:split], X[split:]
    y_tr, y_te = y[:split], y[split:]

    scaler = StandardScaler()
    X_tr   = scaler.fit_transform(X_tr)
    X_te   = scaler.transform(X_te)

    model = LinearRegression()
    model.fit(X_tr, y_tr)

    y_pred  = model.predict(X_te)
    metrics = {
        "city":    city,
        "mae":     round(mean_absolute_error(y_te, y_pred), 2),
        "r2":      round(r2_score(y_te, y_pred), 3),
        "n_train": len(X_tr),
    }

    # Build future timestamps
    last_temps = list(feat_df["temp"].values[-24:])
    last_ts    = feat_df["timestamp"].iloc[-1]
    rows = []

    for h in range(1, hours_ahead + 1):
        fts      = last_ts + datetime.timedelta(hours=h)
        hour     = fts.hour
        dow      = fts.dayofweek
        hs       = np.sin(2 * np.pi * hour / 24)
        hc       = np.cos(2 * np.pi * hour / 24)
        l3       = last_temps[-3]  if len(last_temps) >= 3  else last_temps[-1]
        l6       = last_temps[-6]  if len(last_temps) >= 6  else last_temps[-1]
        l24      = last_temps[-24] if len(last_temps) >= 24 else last_temps[-1]
        roll     = np.mean(last_temps[-24:])

        feat_row = scaler.transform([[hs, hc, dow, l3, l6, l24, roll]])
        pred     = round(float(model.predict(feat_row)[0]), 1)
        rows.append({"timestamp": fts, "predicted_temp": pred, "hour": h})
        last_temps.append(pred)

    return pd.DataFrame(rows), metrics, model


def run_all_forecasts(df: pd.DataFrame, hours_ahead: int = 24):
    all_forecasts, all_metrics = {}, []
    for city in df["city"].unique():
        fdf, met, _ = train_and_forecast(df[df["city"] == city].copy(), city, hours_ahead)
        if fdf is not None:
            all_forecasts[city] = fdf
            all_metrics.append(met)
    return all_forecasts, all_metrics