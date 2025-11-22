import os
import json
from curl_cffi import requests as crequests  # <--- THE NEW WEAPON
import psycopg2
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

# --- CONFIGURATION ---
DATABASE_URL = "postgresql://postgres.nnjctyovtecunurbkhnm:pranav1920@aws-1-ap-northeast-1.pooler.supabase.com:6543/postgres?sslmode=require"
EXTERNAL_API_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"

# --- HELPER FUNCTIONS ---
def get_color(number):
    try:
        n = int(number)
        if n in [0, 5]: return "Violet"
        if n % 2 == 1: return "Green"
        return "Red"
    except:
        return "Unknown"

def get_size(number):
    try:
        n = int(number)
        return "Big" if n >= 5 else "Small"
    except:
        return "Unknown"

def find_value(item, possible_keys):
    for key in possible_keys:
        if key in item and item[key] is not None:
            return item[key]
    return None

# --- MAIN TASK ---
def fetch_and_clean_data():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 1️⃣ START: Job triggered", flush=True)
    conn = None
    try:
        # --- FIX ATTEMPT 3: TLS IMPERSONATION ---
        # We pretend to be Chrome 120 EXACTLY, down to the encryption packet
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 2️⃣ CURL-CFFI: Requesting as Chrome 120...", flush=True)
        
        response = crequests.get(
            EXTERNAL_API_URL,
            impersonate="chrome120", # This mimics a real browser's fingerprint
            timeout=15
        )
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 3️⃣ RESPONSE: Got code {response.status_code}", flush=True)
        
        if response.status_code == 403:
            print("❌ BLOCKED: Still 403. The IP itself is banned.", flush=True)
            return

        if response.status_code != 200:
            print(f"⚠️ ERROR: API returned {response.status_code}", flush=True)
            return
            
        raw_json = response.json()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 4️⃣ PARSE: JSON received", flush=True)

        # Parse Data
        if isinstance(raw_json, list):
            items = raw_json
        elif 'data' in raw_json and isinstance(raw_json['data'], list):
            items = raw_json['data']
        elif 'list' in raw_json and isinstance(raw_json['list'], list):
            items = raw_json['list']
        elif 'data' in raw_json and 'list' in raw_json['data']:
            items = raw_json['data']['list']
        else:
            items = [raw_json]

        # Connect DB
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 5️⃣ DB: Connecting...", flush=True)
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS history (
                period BIGINT PRIMARY KEY,
                draw_time TIMESTAMP,
                winning_number INT,
                result_color TEXT,
                result_size TEXT,
                raw_json JSONB
            );
        """)
        
        saved_count = 0
        for item in items:
            period = find_value(item, ['issueNumber', 'issue', 'period', 'planNo', 'issueNo', 'drawId'])
            number = find_value(item, ['number', 'winningNumber', 'openNumber', 'result', 'winNumber', 'code'])

            if period is not None and number is not None:
                period_int = int(period)
                number_int = int(number)
                color = get_color(number_int)
                size = get_size(number_int)
                
                cur.execute("""
                    INSERT INTO history (period, draw_time, winning_number, result_color, result_size, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (period) DO NOTHING;
                """, (period_int, datetime.now(), number_int, color, size, json.dumps(item)))
                
                if cur.rowcount > 0:
                    saved_count += 1
        
        conn.commit()
        cur.close()
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ FINISHED: Saved {saved_count} new rounds.", flush=True)

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}", flush=True)
    finally:
        if conn:
            conn.close()

# --- SCHEDULER ---
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 LIFESPAN: Starting scheduler...", flush=True)
    scheduler.add_job(fetch_and_clean_data, 'interval', seconds=10)
    scheduler.start()
    yield
    print("🛑 LIFESPAN: Stopping scheduler...", flush=True)
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home():
    return {"message": "Lottery Bot - Attempt 3 (TLS)"}

@app.get("/history")
def get_history():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT period, winning_number, result_size, result_color FROM history ORDER BY period DESC LIMIT 50")
        rows = cur.fetchall()
        
        data = []
        for r in rows:
            data.append({"period": r[0], "number": r[1], "size": r[2], "color": r[3]})
        return data
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()
