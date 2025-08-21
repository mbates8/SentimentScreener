import threading
import io
import os
import json
import dash
from dash import html, dcc, Dash, dash_table, callback_context
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine, text
import plotly.express as px
import queue
import traceback
import time
import pathlib
from flask import send_file, jsonify

# ---------- config ----------
DB_URL = os.getenv("DATABASE_URL",
                   "mysql+mysqlconnector://sentiment_user:eMber0310%21@localhost/stock_data")
POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", 60_000))
DEMO_MAX_SYMBOLS = int(os.getenv("DEMO_MAX_SYMBOLS", 200))
# How long to wait for DB reads (seconds) before falling back
DB_READ_TIMEOUT_S = int(os.getenv("DB_READ_TIMEOUT_S", 30))
# ----------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], title="Sentiment Screener — Safe")
server = app.server  # flask server for status.json route if needed

@server.route("/status.json")
def serve_status_json():
    p = pathlib.Path("status.json")
    if not p.exists():
        return jsonify({"running": False, "active_scripts": [], "last_finviz_update": None})
    return send_file(str(p), mimetype="application/json")

# ---------- DB engine ----------
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    connect_args={
        # client-side connect/read timeouts
        "connect_timeout": 60,
        "read_timeout": 60,
    }
)

# ---------- helpers ----------
def read_watchlist_from_db(conn):
    """
    Return the top 20 tickers by relative_volume using the single most-recent
    row per ticker (by id). This avoids any timezone/date filtering issues.
    """
    sql = """
    SELECT fd.ticker, fd.price_change, fd.relative_volume, fd.avg_volume
    FROM finviz_data fd
    JOIN (
      SELECT ticker, MAX(id) AS max_id
      FROM finviz_data
      GROUP BY ticker
    ) m ON fd.id = m.max_id
    ORDER BY fd.relative_volume DESC
    LIMIT 20;
    """
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"[read_watchlist_from_db] error: {e}")
        return pd.DataFrame(columns=["ticker", "price_change", "relative_volume", "avg_volume"])

def read_scatter_from_db(conn):
    """
    Return a DataFrame with today's daily_sentiment merged with the latest-per-ticker
    finviz_data (relative_volume and price_change). This runs one SQL query (no per-ticker loop).
    `conn` can be a DBAPI/SQLAlchemy connection object accepted by pandas.read_sql.
    """
    import pandas as pd

    sql = """
    SELECT
      ds.ticker                    AS ticker,
      ds.avg_sentiment             AS avg_sentiment,
      fd.relative_volume           AS relative_volume,
      fd.price_change              AS price_change
    FROM daily_sentiment ds
    INNER JOIN (
      -- latest row per ticker from finviz_data
      SELECT fd2.ticker, fd2.relative_volume, fd2.price_change
      FROM finviz_data fd2
      JOIN (
        SELECT ticker, MAX(id) AS max_id
        FROM finviz_data
        GROUP BY ticker
      ) m ON fd2.id = m.max_id
    ) fd ON fd.ticker = ds.ticker
    WHERE ds.date = CURDATE();
    """

    try:
        df = pd.read_sql_query(sql, conn)

        # Ensure consistent columns even when empty
        if df.empty:
            return pd.DataFrame(columns=["ticker", "avg_sentiment", "relative_volume", "price_change"])

        # Normalize column order & types if needed
        df = df[["ticker", "avg_sentiment", "relative_volume", "price_change"]]

        return df

    except Exception as e:
        print(f"[read_scatter_from_db] error: {e}")
        # return an empty frame with the expected columns so callers won't break
        return pd.DataFrame(columns=["ticker", "avg_sentiment", "relative_volume", "price_change"])

def read_news_from_db(conn):
    try:
        df = pd.read_sql("SELECT published_at, ticker, headline FROM news ORDER BY published_at DESC LIMIT 20", conn,
                       parse_dates=["published_at"])
        return df
    except Exception as e:
        print(f"[read_news_from_db] error: {e}")
        return pd.DataFrame(columns=["published_at","ticker","headline"])

def read_last_finviz_update_from_db(conn):
    try:
        df = pd.read_sql("SELECT MAX(inserted_at) AS last_update FROM finviz_data WHERE inserted_at >= CURDATE() AND inserted_at < CURDATE() + INTERVAL 1 DAY", conn)
        if not df.empty and pd.notna(df.loc[0,"last_update"]):
            return pd.to_datetime(df.loc[0,"last_update"])
    except Exception as e:
        print(f"[read_last_finviz_update_from_db] error: {e}")
    return None

# Demo data generators
def demo_watchlist(n=20):
    import numpy as np
    tickers = [f"T{str(i).zfill(3)}" for i in range(1, n+1)]
    relvol = np.round(np.random.rand(n) * 10 + 1, 2)
    avg_vol = (np.random.rand(n) * 5000 + 200).astype(int)
    price_change = np.round(np.random.randn(n) * 5, 2)
    df = pd.DataFrame({"ticker": tickers, "price_change": price_change, "relative_volume": relvol, "avg_volume": avg_vol})
    return df.sort_values("relative_volume", ascending=False).head(20)

def demo_scatter(n=30):
    import numpy as np
    tickers = [f"T{str(i).zfill(3)}" for i in range(1, n+1)]
    avg_sentiment = np.clip(np.random.randn(n)/3, -1, 1)
    relvol = np.round(np.random.rand(n)*10+1, 2)
    price_change = np.round(np.random.randn(n)*6, 2)
    return pd.DataFrame({"ticker": tickers, "avg_sentiment": avg_sentiment, "relative_volume": relvol, "price_change": price_change})

def demo_news(n=20):
    now = datetime.now()
    rows = []
    for i in range(n):
        rows.append({"published_at": now, "ticker": f"T{str(i%10).zfill(3)}", "headline": f"Demo headline #{i+1} — market update"})
    return pd.DataFrame(rows)

# ---------- UI ----------
controls_col = dbc.Card(
    [
        dbc.CardHeader(html.H6("Controls")),
        dbc.CardBody([
            html.Div(id="status-last-update", children="Last finviz update: —", style={"fontSize":"12px", "color":"#333"}),
            html.Br(),
            dbc.Row([
                dbc.Col(html.Div("Auto-refresh:"), width=6),
                dbc.Col(dbc.Switch(id="pause-switch", value=False, label="Pause"), width=6)
            ], align="center"),
            html.Br(),
            dbc.Row([
                dbc.Col(html.Div("Data source:"), width=6),
                dbc.Col(dcc.Dropdown(id="data-source", options=[
                    {"label":"Live (DB)", "value":"live"},
                    {"label":"Demo", "value":"demo"},
                ], value="live", clearable=False, style={"fontSize":"12px"}), width=6),
            ]),
            html.Br(),
            dbc.Row([
                dbc.Col(dbc.Button("Refresh Now", id="refresh-now", color="primary", size="sm"), width=6),
                dbc.Col(dbc.Button("Export CSV", id="export-csv", color="secondary", size="sm"), width=6),
            ]),
            html.Div(id="export-done", style={"fontSize":"11px", "color":"green", "marginTop":"6px"})
        ])
    ],
    className="mb-3"
)
watchlist_card = dbc.Card(
    [dbc.CardHeader(html.H6("Watchlist — Top RelVol")),
     dbc.CardBody(dash_table.DataTable(
         id="watchlist-table",
         columns=[{"name": c, "id": c} for c in ["ticker", "price_change", "relative_volume", "avg_volume"]],
         style_table={"overflowY": "auto", "maxHeight": "55vh"},
         style_cell={"textAlign": "center", "padding": "6px"},
         style_header={"backgroundColor": "#007bff", "color": "white", "fontWeight": "bold"},
         page_action="none", fixed_rows={"headers": True},
         style_data_conditional=[
             {"if": {"column_id": "relative_volume"}, "fontWeight": "600"},
             {"if": {"filter_query": "{price_change} > 0", "column_id":"price_change"}, "color":"green"},
             {"if": {"filter_query": "{price_change} < 0", "column_id":"price_change"}, "color":"red"},
         ],
     ))]
)
plot_card = dbc.Card(
    [dbc.CardHeader(html.H6("3D: Sentiment vs RelVol vs ΔPrice")),
     dbc.CardBody(dcc.Loading(dcc.Graph(id="sentiment-3d", style={"height":"60vh"}), type="circle"))]
)
news_card = dbc.Card(
    [dbc.CardHeader(html.H6("News Feed (Latest 20)")),
     dbc.CardBody(html.Div(id="news-feed", style={"overflowY":"auto", "maxHeight":"65vh", "fontSize":"13px"}))])

app.layout = dbc.Container([
    dbc.Row([dbc.Col(html.H1("📊 Sentiment Screener"), width=9),
             dbc.Col(html.Div(id="top-right-small", style={"textAlign":"right", "fontSize":"12px", "color":"#666"}), width=3)],
            className="my-2"),
    dbc.Row([dbc.Col(controls_col, width=3), dbc.Col(plot_card, width=6), dbc.Col(news_card, width=3)]),
    dbc.Row([dbc.Col(watchlist_card, width=12)], className="mt-3"),
    dcc.Interval(id="interval", interval=POLL_INTERVAL_MS, n_intervals=0),
    dcc.Download(id="download-watchlist"),
    dcc.Store(id="last-output-store"),
    dcc.Store(id="last-update-ts", data=None),
], fluid=True)

# ---------- DB worker + improved retry handling ----------
def _db_reads_worker(q_out):
    """Worker that performs all DB reads and places result dict in q_out."""
    try:
        with engine.connect() as conn:
            df_w = read_watchlist_from_db(conn)
            df_c = read_scatter_from_db(conn)
            df_n = read_news_from_db(conn)
            last_f = read_last_finviz_update_from_db(conn)
        q_out.put({"ok": True, "watch": df_w, "scatter": df_c, "news": df_n, "last": last_f})
    except Exception as e:
        q_out.put({"ok": False, "error": str(e), "trace": traceback.format_exc()})

def run_db_reads_with_timeout(timeout_s=DB_READ_TIMEOUT_S):
    """
    Try DB reads with timeout. We attempt twice with a short backoff to reduce false failures.
    If a read fails due to a connection error, dispose the engine pool to reset connections.
    """
    tries = 2
    for attempt in range(1, tries+1):
        q = queue.Queue()
        t = threading.Thread(target=_db_reads_worker, args=(q,), daemon=True)
        t.start()
        try:
            result = q.get(timeout=timeout_s)
            if result.get("ok"):
                return result
            else:
                print(f"[run_db_reads_with_timeout] attempt {attempt} DB error: {result.get('error')}")
                print(result.get("trace", ""))
                # dispose pool to avoid reusing bad connections
                try:
                    engine.dispose()
                    print("[run_db_reads_with_timeout] disposed engine pool to reset connections")
                except Exception as _:
                    pass
        except queue.Empty:
            print(f"[run_db_reads_with_timeout] attempt {attempt} timed out after {timeout_s}s")
            # if we timed out, dispose pool and retry
            try:
                engine.dispose()
                print("[run_db_reads_with_timeout] disposed engine pool after timeout")
            except Exception:
                pass
        if attempt < tries:
            time.sleep(1.0 + attempt * 0.5)
    return {"ok": False, "error": f"DB read failed after {tries} attempts (each {timeout_s}s)"}

# ---------- main callback ----------
@app.callback(
    Output("watchlist-table", "data"),
    Output("sentiment-3d", "figure"),
    Output("news-feed", "children"),
    Output("status-last-update", "children"),
    Output("last-output-store", "data"),
    Input("interval", "n_intervals"),
    Input("pause-switch", "value"),
    Input("data-source", "value"),
    Input("refresh-now", "n_clicks"),
    State("last-output-store", "data"),
    prevent_initial_call=False
)
def update_everything(n_intervals, paused, data_source, refresh_clicks, cached):
    trigger = callback_context.triggered[0]["prop_id"] if callback_context.triggered else None

    if paused:
        if cached:
            return cached["watch"], cached["fig"], cached["news"], cached["last_text"], cached

    if data_source == "demo":
        df_w = demo_watchlist(20)
        df_c = demo_scatter(30)
        df_n = demo_news(20)
        last_f = datetime.now()
    else:
        res = run_db_reads_with_timeout(timeout_s=DB_READ_TIMEOUT_S)
        if res.get("ok"):
            df_w = res["watch"]
            df_c = res["scatter"]
            df_n = res["news"]
            last_f = res["last"]
        else:
            print(f"[update_everything] DB read failed: {res.get('error')}")
            if cached and all(k in cached for k in ("watch","fig","news","last_text")):
                print("[update_everything] Using cached UI payload")
                return cached["watch"], cached["fig"], cached["news"], cached["last_text"], cached
            df_w = demo_watchlist(20)
            df_c = demo_scatter(30)
            df_n = demo_news(20)
            last_f = None

    for col in ["price_change", "relative_volume", "avg_volume"]:
        if col in df_w:
            df_w[col] = pd.to_numeric(df_w[col], errors="coerce")
    watch_data = df_w.to_dict("records")

    # build nicer 3d figure
    if df_c.empty:
        fig = {"data": [], "layout": {"annotations":[{"text":"No data for today yet","xref":"paper","yref":"paper","showarrow":False}]}}
    else:
        try:
            # compute sensible size scaling
            max_rel = max(1.0, float(df_c["relative_volume"].dropna().max() if "relative_volume" in df_c else 1.0))
            # use size_max to cap marker size
            fig = px.scatter_3d(df_c, x="avg_sentiment", y="relative_volume", z="price_change",
                                size="relative_volume", size_max=28,
                                color="avg_sentiment", color_continuous_scale="RdYlBu",
                                range_color=[-1,1],
                                hover_name="ticker",
                                title=f"Sentiment vs RelVol vs ΔPrice ({date.today()})",
                                labels={"avg_sentiment":"avg_sentiment","relative_volume":"relative_volume","price_change":"price_change"})
            fig.update_traces(marker=dict(symbol="circle", line=dict(width=0.3, color="DarkSlateGrey"), opacity=0.8))
            fig.update_layout(scene=dict(
                xaxis=dict(range=[-1, +1], title="avg_sentiment"),
                yaxis=dict(title="relative_volume"),
                zaxis=dict(title="price_change")
            ), margin=dict(l=40,r=40,t=60,b=20))
        except Exception as e:
            print(f"[update_everything] error building 3d fig: {e}")
            fig = {"data": [], "layout": {}}

    news_children = []
    if not df_n.empty:
        for _, row in df_n.iterrows():
            ts = row.published_at.strftime("%H:%M:%S") if pd.notna(row.published_at) else ""
            news_children.append(
                html.Div([
                    html.Span(f"[{ts}] ", style={"fontWeight":"700", "marginRight":"6px"}),
                    html.Span(f"{row.ticker}: ", style={"fontWeight":"600"}),
                    html.Span(row.headline)
                ], style={"padding":"6px 2px", "borderBottom":"1px solid #eee"})
            )
    else:
        news_children.append(html.Div("No recent news found", style={"color":"#666"}))

    last_text = f"Last finviz update: {last_f.strftime('%Y-%m-%d %H:%M:%S')}" if last_f else "Last finviz update: N/A"

    cache_obj = {"watch": watch_data, "fig": fig, "news": news_children, "last_text": last_text}
    return watch_data, fig, news_children, last_text, cache_obj

# ---------- Export CSV ----------
@app.callback(
    Output("download-watchlist", "data"),
    Input("export-csv", "n_clicks"),
    State("last-output-store", "data"),
    prevent_initial_call=True
)
def export_csv(n_clicks, cached):
    if not cached or "watch" not in cached:
        return dash.no_update
    df = pd.DataFrame(cached["watch"])
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return dcc.send_string(buf.getvalue(), filename=f"watchlist_{date.today().isoformat()}.csv")

@app.callback(
    Output("top-right-small", "children"),
    Input("interval", "n_intervals"),
    Input("data-source", "value")
)
def top_right_text(n, data_source):
    return html.Div([
        html.Div(f"Data source: {data_source.capitalize()}", style={"fontSize":"12px"}),
        html.Div(f"Polling every {POLL_INTERVAL_MS//1000}s", style={"fontSize":"11px", "color":"#666"})
    ])

# ---------- run ----------
if __name__ == "__main__":
    # disable reloader to avoid duplicate processes & connections
    app.run(debug=False, use_reloader=False, port=8050)
