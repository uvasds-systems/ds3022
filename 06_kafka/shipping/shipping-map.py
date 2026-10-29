"""
Shipping Map

Streamlit app that reads vessel state and tracks from Redis (written by
shipping-consumer.py) and draws each ship's recent path as a fading trail
(TripsLayer) with a heading arrow at its latest position.

Run with:  streamlit run 06_kafka/shipping/shipping-map.py
"""

import os
import time
import base64
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import pydeck as pdk
import redis
import streamlit as st

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
TRAIL_SECONDS = 24 * 60 * 60  # how far back the fade reaches; match TRACK_SECONDS in the consumer
ACTIVE_SECONDS = 2 * 60 * 60  # ships silent longer than this keep their trail but lose their arrow

# AIS ship type codes -> (label, RGB color)
CATEGORIES = [
    (range(70, 80), "Cargo", [66, 165, 245]),
    (range(80, 90), "Tanker", [239, 83, 80]),
    (range(60, 70), "Passenger", [171, 71, 188]),
    ([31, 32, 52], "Tug / towing", [255, 167, 38]),
    (range(30, 31), "Fishing", [102, 187, 106]),
    (range(36, 38), "Sailing / pleasure", [38, 198, 218]),
]
OTHER = ("Other / unknown", [189, 189, 189])

# White arrow pointing north; mask=True lets deck.gl tint it with each ship's color
ARROW_SVG = ('<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">'
             '<polygon points="32,4 54,60 32,46 10,60" fill="white"/></svg>')
ARROW = {
    "url": "data:image/svg+xml;base64," + base64.b64encode(ARROW_SVG.encode()).decode(),
    "width": 64, "height": 64, "anchorY": 32, "mask": True,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    # Rotate at 10 MB (2 backups); stdout too, so `kubectl logs` shows map errors
    handlers=[RotatingFileHandler("shipping-map.log", maxBytes=10_000_000, backupCount=2),
              logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# One Redis connection shared across reruns and browser sessions
@st.cache_resource
def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


# Map an AIS type code to its category label and color
def category(code):
    try:
        code = int(float(code))
    except (TypeError, ValueError):
        return OTHER
    return next(((label, color) for rng, label, color in CATEGORIES if code in rng), OTHER)


# Load every ship hash plus its track (ordered oldest -> newest) in two round trips
def load_ships(r):
    keys = list(r.scan_iter("ship:*"))
    pipe = r.pipeline()
    for k in keys:
        pipe.hgetall(k)
    ships = pipe.execute()
    for k in keys:
        pipe.zrange(f"track:{k.split(':', 1)[1]}", 0, -1, withscores=True)
    tracks = pipe.execute()

    rows = []
    for ship, track in zip(ships, tracks):
        if "lat" not in ship:  # static data seen, but no position yet
            continue
        label, color = category(ship.get("type"))
        path = [[float(x) for x in pt.split(",")] for pt, _ in track]
        times = [ts for _, ts in track]
        # Track points are 500 m apart, so extend the trail to the ship's latest position
        if not times or float(ship["time"]) > times[-1]:
            path.append([float(ship["lon"]), float(ship["lat"])])
            times.append(float(ship["time"]))
        rows.append({
            **ship,
            "path": path,
            "times": times,
            "category": label,
            "color": color,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["lat", "lon", "time", "sog", "heading"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[["sog", "heading"]] = df[["sog", "heading"]].fillna(0)
    df["angle"] = -df["heading"]  # deck.gl rotates counter-clockwise; heading is clockwise
    df["age_min"] = ((time.time() - df["time"]) / 60).round(1)
    # Ships without static data yet have no destination; NaN would break the deck's JSON
    df["destination"] = df.get("destination", pd.Series(index=df.index, dtype=str)).fillna("")
    return df


# Fading trails for every ship + heading arrows for active ones; timestamps are made relative
# to `base` because deck.gl stores them as 32-bit floats, which can't hold unix seconds precisely
def ship_map(df, active):
    now = time.time()
    base = now - TRAIL_SECONDS
    trips = df[df["path"].str.len() >= 2].copy()
    trips["timestamps"] = trips["times"].apply(lambda ts: [t - base for t in ts])

    trail_layer = pdk.Layer(
        "TripsLayer",
        data=trips[["path", "timestamps", "color"]],
        get_path="path",
        get_timestamps="timestamps",
        get_color="color",
        width_min_pixels=2.5,
        cap_rounded=True,
        joint_rounded=True,
        opacity=0.9,
        trail_length=TRAIL_SECONDS,
        current_time=now - base,
    )
    ship_layer = pdk.Layer(
        "IconLayer",
        # Only the columns the layer and tooltip use, so no NaN reaches the browser
        data=active[["lon", "lat", "angle", "color", "name", "category", "sog", "heading",
                     "destination", "age_min"]].assign(icon=[ARROW] * len(active)),
        get_icon="icon",
        get_position=["lon", "lat"],
        get_angle="angle",
        get_color="color",
        get_size=16,
        size_units="'pixels'",  # quoted: pydeck treats a bare string as a JS expression
        pickable=True,
    )
    return pdk.Deck(
        layers=[trail_layer, ship_layer],
        initial_view_state=pdk.ViewState(latitude=38.2, longitude=-76.3, zoom=7.2),
        map_style=pdk.map_styles.CARTO_DARK,
        tooltip={"text": "{name} ({category})\n{sog} kn, heading {heading}°\n"
                         "Dest: {destination}\nLast report {age_min} min ago",
                 "style": {"color": "white", "backgroundColor": "#1e2a38", "fontFamily": "Roboto, sans-serif"}},
    )


st.set_page_config(page_title="Chesapeake Shipping", layout="wide")
# Trim Streamlit's default ~6rem top padding above the title
st.markdown("<style>[data-testid='stMainBlockContainer']{padding-top:2rem}</style>",
            unsafe_allow_html=True)
st.title("Chesapeake Bay Ship Traffic (AISStream)")


# Re-runs only this block every 30 seconds, not the whole script
@st.fragment(run_every="30s")
def live_map():
    try:
        df = load_ships(get_redis())
    except redis.RedisError as e:
        log.error(f"Redis read failed: {e}")
        st.error(f"Cannot reach Redis at {REDIS_HOST}:6379 — {e}")
        return
    if df.empty:
        st.info("No ships in Redis yet. Are shipping-producer.py and shipping-consumer.py running?")
        return

    active = df[df["age_min"] <= ACTIVE_SECONDS / 60]
    c1, c2, c3 = st.columns(3)
    c1.metric("Active ships (last 2h)", len(active))
    c2.metric("Underway (> 1 kn)", int((active["sog"] > 1).sum()))
    c3.metric("Track points (24h)", int(df["path"].str.len().sum()))

    st.pydeck_chart(ship_map(df, active), height=700)
    # Color legend for ship categories
    legend = {label: c for _, label, c in CATEGORIES} | {OTHER[0]: OTHER[1]}
    st.markdown(" &nbsp; ".join(
        f"<span style='color:rgb({c[0]},{c[1]},{c[2]})'>■</span> {label}" for label, c in legend.items()
    ), unsafe_allow_html=True)
    cols = ["name", "category", "sog", "heading", "destination", "age_min"]
    st.dataframe(active.sort_values("age_min")[cols].head(50), hide_index=True)


live_map()
