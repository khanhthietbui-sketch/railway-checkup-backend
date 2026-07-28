import sqlite3, os, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# 使用 /tmp 目录，Railway 环境可写
DB_PATH = "/tmp/records.db"
JST = timedelta(hours=9)
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
    print(f"数据库初始化失败: {e}", file=sys.stderr)

app = FastAPI(title="查岗系统")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class ReportBody(BaseModel):
    app_name: str
    event: str

@app.post("/report")
async def report(body: ReportBody, req: Request):
    try:
        auth = req.headers.get("Authorization", "")
        if AUTH_TOKEN and auth != f"Bearer {AUTH_TOKEN}":
            raise HTTPException(401, "Unauthorized")
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO records (app_name, event, timestamp) VALUES (?, ?, ?)",
                     (body.app_name, body.event, now))
        conn.commit()
        conn.close()
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"写入失败: {str(e)}")

@app.get("/ping")
async def ping():
    return "pong"

@app.get("/activity/summary")
async def summary():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT app_name, event, timestamp FROM records ORDER BY id DESC LIMIT 5")
        recent = cur.fetchall()
        cur.execute("SELECT app_name, event, timestamp FROM records ORDER BY id ASC")
        rows = cur.fetchall()
        conn.close()
        sessions, opens = {}, {}
        for r in rows:
            app_name, ev, ts = r
            if ev == "open":
                opens[app_name] = datetime.fromisoformat(ts)
            elif ev == "close" and app_name in opens:
                gap = int((datetime.fromisoformat(ts) - opens[app_name]).total_seconds())
                sessions[app_name] = sessions.get(app_name, 0) + gap
                del opens[app_name]
        return {
            "recent_apps": [r[0] for r in recent],
            "sessions": sessions
        }
    except Exception as e:
        raise HTTPException(500, f"查询失败: {str(e)}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
