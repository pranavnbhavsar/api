import os
import json
import cloudscraper # <--- NEW LIBRARY
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

# --- MAIN TASK: FETCH & SAVE ---
def fetch_and_clean_data():
    conn = None
    try:
        # --- FIX FOR 403: USE CLOUDSCRAPER ---
        # This creates a sophisticated browser session that solves blocks
        scraper = cloudscraper.create_scraper() 
        
        response = scraper.get(EXTERNAL_API_URL, timeout=15)
        
        if response.status_code == 403:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ FATAL BLOCK (403): Even Cloudscraper was blocked.")
            return

        if response.status_code != 200:
            print(f"⚠️ API Error: {response.status_code}")
            return
            
        raw_json = response.json()

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

        # Connect to Database
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
        
        if saved_count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Saved {saved_count} new rounds.")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] .")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn:
            conn.close()

# --- SCHEDULER SETUP ---
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting 10-second background scraper...")
    scheduler.add_job(fetch_and_clean_data, 'interval', seconds=10)
    scheduler.start()
    yield
    print("🛑 Stopping background scraper...")
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home():
    return {"message": "Lottery Bot Running with Cloudscraper"}

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
