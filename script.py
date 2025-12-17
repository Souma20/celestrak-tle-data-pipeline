import requests
import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"
DATABASE_URL = os.getenv("DATABASE_URL")

# --- HELPER: B-STAR PARSER ---
def parse_bstar(bstar_string):
    """Converts '44559-4' string to float 0.000044559"""
    try:
        bstar_string = bstar_string.strip()
        if '-' in bstar_string[-2:] or '+' in bstar_string[-2:]:
            mantissa = bstar_string[:-2]
            exponent = bstar_string[-2:]
            return float(f"0.{mantissa}") * (10 ** int(exponent))
        return float(bstar_string)
    except:
        return None

# --- HELPER: TLE PARSER ---
def parse_tle_pair(line1, line2, sat_name, fetched_at):
    try:
        # --- LINE 1 ---
        norad_id = int(line1[2:7])
        
        # [FIX 1] Extract International Designator (Columns 9-17)
        intl_des = line1[9:17].strip()
        
        epoch_year = int(line1[18:20])
        epoch_day = float(line1[20:32])
        full_year = 2000 + epoch_year if epoch_year < 57 else 1900 + epoch_year
        epoch_date = datetime.datetime(full_year, 1, 1) + datetime.timedelta(days=epoch_day - 1)
        
        raw_bstar = line1[53:61].strip()
        bstar_val = parse_bstar(raw_bstar)

        # --- LINE 2 ---
        inclination = float(line2[8:16])
        raan = float(line2[17:25])
        eccentricity = float("0." + line2[26:33])
        arg_perigee = float(line2[34:42])
        mean_anomaly = float(line2[43:51])
        mean_motion = float(line2[52:63])
        rev_number = int(line2[63:68])

        return {
            'norad_id': norad_id,
            'sat_name': sat_name,
            'intl_designator': intl_des,  # Added this field
            'epoch_utc': epoch_date,
            'fetched_at_utc': fetched_at,
            'inclination': inclination,
            'raan': raan,
            'eccentricity': eccentricity,
            'arg_perigee': arg_perigee,
            'mean_anomaly': mean_anomaly,
            'mean_motion': mean_motion,
            'b_star_drag': bstar_val,
            'rev_number': rev_number
        }
    except Exception as e:
        print(f"Error parsing TLE for {sat_name}: {e}")
        return None

def fetch_and_save_to_db():
    print(f"Fetching data from {URL}...")
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Network error: {e}")
        return

    lines = response.text.strip().splitlines()
    fetched_at = datetime.datetime.utcnow()
    records = []

    # Parse Loop
    for i in range(0, len(lines), 3):
        if i + 2 < len(lines):
            name = lines[i].strip()
            l1 = lines[i+1].strip()
            l2 = lines[i+2].strip()
            parsed = parse_tle_pair(l1, l2, name, fetched_at)
            if parsed:
                records.append(parsed)

    if not records:
        print("No valid records found.")
        return

    # --- SAVE TO DATABASE ---
    print(f"Parsed {len(records)} records. Connecting to DB...")
    
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is missing!")
        return
        
    engine = create_engine(DATABASE_URL)
    df = pd.DataFrame(records)

    # --- [FIX 2] SMART FILTERING (Prevents Crashes) ---
    
    # 1. Update Satellites Table
    # We fetch existing IDs so we don't try to insert duplicates (which crashes the script)
    with engine.connect() as conn:
        existing_ids = pd.read_sql("SELECT norad_id FROM dim_satellites", conn)
    
    # Filter df to only NEW satellites
    new_sats = df[~df['norad_id'].isin(existing_ids['norad_id'])]
    unique_new_sats = new_sats[['norad_id', 'sat_name', 'intl_designator']].drop_duplicates(subset=['norad_id'])

    if not unique_new_sats.empty:
        print(f"found {len(unique_new_sats)} new satellites. Saving...")
        unique_new_sats.to_sql('dim_satellites', engine, if_exists='append', index=False)
    else:
        print("No new satellites found.")

    # 2. Save Telemetry
    # We want to avoid duplicates. Since we have thousands of rows, we can't query everything.
    # STRATEGY: We just try to append. If it fails, we catch it. 
    # (Smart Filtering for 7000 rows is expensive, let's trust the database constraints + chunking)
    
    fact_telem = df[[
        'norad_id', 'epoch_utc', 'fetched_at_utc', 'inclination', 
        'raan', 'eccentricity', 'arg_perigee', 'mean_anomaly', 
        'mean_motion', 'b_star_drag', 'rev_number'
    ]]

    # We use a trick: read the MOST RECENT data from DB to filter obvious duplicates from this batch
    try:
        # Get data from the last 3 days to compare
        query = text("SELECT norad_id, epoch_utc FROM fact_telemetry WHERE epoch_utc > NOW() - INTERVAL '3 days'")
        with engine.connect() as conn:
            recent_data = pd.read_sql(query, conn)
        
        # Create a 'composite key' for filtering
        recent_data['key'] = recent_data['norad_id'].astype(str) + "_" + pd.to_datetime(recent_data['epoch_utc']).astype(str)
        fact_telem['key'] = fact_telem['norad_id'].astype(str) + "_" + pd.to_datetime(fact_telem['epoch_utc']).astype(str)
        
        # Filter out rows that already exist
        new_telemetry = fact_telem[~fact_telem['key'].isin(recent_data['key'])].copy()
        new_telemetry = new_telemetry.drop(columns=['key']) # Clean up
        
        if not new_telemetry.empty:
            print(f"Saving {len(new_telemetry)} new telemetry records...")
            new_telemetry.to_sql('fact_telemetry', engine, if_exists='append', index=False, chunksize=1000)
            print("✅ Success!")
        else:
            print("No new telemetry data (all duplicates).")

    except Exception as e:
        print(f"⚠️ Error during save: {e}")

if __name__ == "__main__":
    fetch_and_save_to_db()