import os
import json
import requests
import psycopg2
from datetime import datetime
from fastapi import FastAPI

# --- CONFIGURATION ---
# REPLACE [YOUR_PASSWORD] WITH YOUR REAL SUPABASE PASSWORD
DATABASE_URL = "postgresql://postgres:pranav1920@db.nnjctyovtecunurbkhnm.supabase.co:5432/postgres"

EXTERNAL_API_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"

app = FastAPI()

# --- HELPER: CALCULATE WINNING DETAILS ---
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

# --- HELPER: SMART KEY FINDER ---
# This function looks for the data even if the API changes names
def find_value(item, possible_keys):
    for key in possible_keys:
        if key in item and item[key] is not None:
            return item[key]
    return None

# --- THE CLEAN FETCH FUNCTION ---
def fetch_and_clean_data():
    try:
        # 1. Fetch from API
        response = requests.get(EXTERNAL_API_URL, timeout=10)
        raw_json = response.json()

        # 2. Find the List of Data
        # Some APIs put data in 'data', some in 'list', some just send the list directly.
        if isinstance(raw_json, list):
            items = raw_json
        elif 'data' in raw_json and isinstance(raw_json['data'], list):
            items = raw_json['data']
        elif 'list' in raw_json and isinstance(raw_json['list'], list):
            items = raw_json['list']
        elif 'data' in raw_json and 'list' in raw_json['data']:
            # Double nested: data -> list
            items = raw_json['data']['list']
        else:
            # Fallback: Wrap the whole thing in a list
            items = [raw_json]

        # --- DEBUG SPY: PRINT THE FIRST ITEM ---
        if len(items) > 0:
            print("\n---------- DEBUG: DATA FROM API ----------")
            print("Keys found:", list(items[0].keys()))
            print("Sample Row:", items[0])
            print("------------------------------------------\n")

        # 3. Connect to Database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create Table (Safe to run every time)
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
            # 4. SMART SEARCH for Period and Number
            # We check ALL these common names automatically
            period = find_value(item, ['issueNumber', 'issue', 'period', 'planNo', 'issueNo', 'drawId'])
            number = find_value(item, ['number', 'winningNumber', 'openNumber', 'result', 'winNumber', 'code'])

            # Only save if we found both critical pieces of data
            if period is not None and number is not None:
                period_int = int(period)
                number_int = int(number)
                color = get_color(number_int)
                size = get_size(number_int)
                
                # 5. UPSERT (Save only if new)
                cur.execute("""
                    INSERT INTO history (period, draw_time, winning_number, result_color, result_size, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (period) DO NOTHING;
                """, (period_int, datetime.now(), number_int, color, size, json.dumps(item)))
                
                if cur.rowcount > 0:
                    saved_count += 1
            else:
                print(f"⚠️ SKIPPED: Could not find 'period' or 'number' in item: {item}")

        conn.commit()
        cur.close()
        conn.close()
        
        return f"Success: Scan finished. Saved {saved_count} new rounds."

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return f"Error: {e}"

# --- API ENDPOINTS ---
@app.get("/")
def home():
    return {"message": "Smart Lottery API is Running"}

@app.get("/update")
def trigger():
    return {"status": fetch_and_clean_data()}

@app.get("/history")
def get_history():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT period, winning_number, result_size, result_color FROM history ORDER BY period DESC LIMIT 50")
        rows = cur.fetchall()
        conn.close()
        
        data = []
        for r in rows:
            data.append({"period": r[0], "number": r[1], "size": r[2], "color": r[3]})
        return data
    except Exception as e:
        return {"error": str(e)}