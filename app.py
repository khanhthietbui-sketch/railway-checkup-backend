import sqlite3, os, sys, asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

DB_PATH = "/tmp/records.db"
JST = timedelta(hours=9)
CN_TZ = timezone(timedelta(hours=8))  # 北京时间
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        app_name TEXT NOT NULL,
        event TEXT NOT NULL,
        timestamp TEXT NOT NULL)""")
    conn.commit()
    conn.close()

try:
    init_db()
except Exception as e:
    print(f"DB init failed: {e}", file=sys.stderr)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def require_auth(req: Request):
    if AUTH_TOKEN:
        auth = req.headers.get("Authorization", "")
        if auth != f"Bearer {AUTH_TOKEN}":
            raise HTTPException(401, "Unauthorized")

app = FastAPI(title="查岗系统")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class ReportBody(BaseModel):
    app_name: str
    event: str

async def daily_reset():
    """每天北京时间 0 点整清空全部查岗记录，避免时长跨天无限叠加。"""
    while True:
        now = datetime.now(CN_TZ)
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_secs = max((next_midnight - now).total_seconds(), 1)
        await asyncio.sleep(wait_secs)
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("DELETE FROM records")
            conn.commit()
            conn.close()
            print(f"[daily_reset] {datetime.now(CN_TZ)} 已清空查岗记录，开始新一天计时", file=sys.stderr)
        except Exception as e:
            print(f"[daily_reset] 清空失败: {e}", file=sys.stderr)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(daily_reset())
    print(f"[startup] 每日0点自动重置已开启（北京时间）", file=sys.stderr)

@app.post("/report")
async def report(body: ReportBody, req: Request):
    require_auth(req)
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute("INSERT INTO records (app_name, event, timestamp) VALUES (?, ?, ?)",
                 (body.app_name, body.event, now))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.get("/ping")
async def ping():
    return "pong"

@app.post("/activity/clean")
async def clean_empty(req: Request):
    require_auth(req)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE TRIM(app_name) = ''")
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return {"status": "ok", "deleted": deleted}

@app.get("/activity/summary")
async def summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT app_name, event, timestamp FROM records ORDER BY id DESC LIMIT 5")
    recent = cur.fetchall()
    cur.execute("SELECT app_name, event, timestamp FROM records ORDER BY id ASC")
    rows = cur.fetchall()
    conn.close()
    sessions, opens = {}, {}
    for r in rows:
        app_name, ev, ts = r["app_name"], r["event"], r["timestamp"]
        if not app_name.strip():
            continue
        if ev == "open":
            opens[app_name] = datetime.fromisoformat(ts)
        elif ev == "close" and app_name in opens:
            gap = int((datetime.fromisoformat(ts) - opens[app_name]).total_seconds())
            sessions[app_name] = sessions.get(app_name, 0) + gap
            del opens[app_name]
    return {"recent_apps": [r["app_name"] for r in recent if r["app_name"].strip()], "sessions": sessions}

@app.get("/activity/trend")
async def activity_trend(days: int = Query(3, description="回溯天数")):
    conn = get_db()
    cur = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cur.execute("SELECT app_name, event, timestamp FROM records WHERE timestamp >= ? ORDER BY timestamp", (cutoff,))
    rows = cur.fetchall()
    conn.close()
    freq = {}
    for r in rows:
        ts = datetime.fromisoformat(r["timestamp"])
        day_key = ts.astimezone(timezone.utc).strftime("%Y-%m-%d")
        app = r["app_name"]
        if not app.strip():
            continue
        if day_key not in freq:
            freq[day_key] = {}
        freq[day_key][app] = freq[day_key].get(app, 0) + 1
    return {"trend": freq, "days": days}

@app.get("/activity/idle")
async def idle_check():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT timestamp FROM records ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"idle_hours": None, "message": "暂无记录"}
    last_ts = datetime.fromisoformat(row["timestamp"])
    now = datetime.now(timezone.utc)
    idle_secs = (now - last_ts).total_seconds()
    return {"idle_hours": round(idle_secs / 3600, 2), "last_activity": last_ts.isoformat()}

@app.get("/activity/daily")
async def daily_summary(date_str: str = Query(None, description="日期 YYYY-MM-DD，默认今天")):
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_db()
    cur = conn.cursor()
    start = date_str + "T00:00:00"
    end = date_str + "T23:59:59"
    cur.execute("SELECT app_name, event, timestamp FROM records WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp", (start, end))
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return {"date": date_str, "apps": [], "total_usage_secs": 0, "message": "当天无记录"}
    sessions, opens, app_list = {}, {}, []
    for r in rows:
        an, ev, ts = r["app_name"], r["event"], r["timestamp"]
        if not an.strip():
            continue
        if an not in app_list:
            app_list.append(an)
        if ev == "open":
            opens[an] = datetime.fromisoformat(ts)
        elif ev == "close" and an in opens:
            gap = int((datetime.fromisoformat(ts) - opens[an]).total_seconds())
            sessions[an] = sessions.get(an, 0) + gap
            del opens[an]
    total = sum(sessions.values())
    apps_detail = sorted([{"app": k, "secs": v} for k, v in sessions.items()], key=lambda x: x["secs"], reverse=True)
    return {"date": date_str, "apps": app_list, "usage": apps_detail, "total_usage_secs": total}

@app.get("/status")
async def server_status():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM records")
    total_records = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(DISTINCT app_name) as cnt FROM records WHERE TRIM(app_name) != ''")
    unique_apps = cur.fetchone()["cnt"]
    cur.execute("SELECT timestamp FROM records ORDER BY id DESC LIMIT 1")
    last_row = cur.fetchone()
    conn.close()
    return {
        "status": "running",
        "total_records": total_records,
        "unique_apps": unique_apps,
        "last_record": last_row["timestamp"] if last_row else None
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
