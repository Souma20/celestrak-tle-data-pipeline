import requests
import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
TLE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"
WEATHER_URL = "https://services.swpc.noaa.gov/products/10cm-flux-30-day.json"
DATABASE_URL = os.getenv("DATABASE_URL")

# --- HELPER 1: B-STAR PARSER ---
def parse_bstar(bstar_string):
    try:
        bstar_string = bstar_string.strip()
        if '-' in bstar_string[-2:] or '+' in bstar_string[-2:]:
            mantissa = bstar_string[:-2]
            exponent = bstar_string[-2:]
            return float(f"0.{mantissa}") * (10 ** int(exponent))
        return float(bstar_string)
    except:
        return None

# --- HELPER 2: TLE PARSER ---
def parse_tle_pair(line1, line2, sat_name, fetched_at):
    try:
        # LINE 1
        norad_id = int(line1[2:7])
        intl_des = line1[9:17].strip()
        epoch_year = int(line1[18:20])
        epoch_day = float(line1[20:32])
        full_year = 2000 + epoch_year if epoch_year < 57 else 1900 + epoch_year
        epoch_date = datetime.datetime(full_year, 1, 1) + datetime.timedelta(days=epoch_day - 1)
        
        raw_bstar = line1[53:61].strip()
        bstar_val = parse_bstar(raw_bstar)

        # LINE 2
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
            'intl_designator': intl_des,
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

# --- FUNCTION: FETCH SOLAR WEATHER ---
def fetch_space_weather(engine):
    print("☀️ Fetching Space Weather (NOAA F10.7)...")
    try:
        r = requests.get(WEATHER_URL, timeout=10)
        data = r.json()
        
        records = []
        # [FIX 2] Parse only the 2 columns NOAA provides
        for row in data[1:]: 
            dt_str = row[0].split(" ")[0] # "2023-12-01"
            flux = float(row[1])
            records.append({'date_utc': dt_str, 'f10_7_flux': flux})
            
        df_weather = pd.DataFrame(records)
        df_daily = df_weather.groupby('date_utc')['f10_7_flux'].max().reset_index()

        with engine.connect() as conn:
            existing = pd.read_sql("SELECT date_utc::text FROM fact_space_weather", conn)
        
        new_weather = df_daily[~df_daily['date_utc'].isin(existing['date_utc'])]
        
        if not new_weather.empty:
            new_weather.to_sql('fact_space_weather', engine, if_exists='append', index=False)
            print(f"✅ Saved {len(new_weather)} new days of Solar Data.")
        else:
            print("☀️ Solar Weather is up to date.")
            
    except Exception as e:
        print(f"⚠️ Space Weather Error: {e}")

# --- MAIN EXECUTOR ---
def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is missing!")
        return
    
    engine = create_engine(DATABASE_URL)
    
    # 1. RUN WEATHER FETCH
    fetch_space_weather(engine)

    # 2. RUN TLE FETCH
    print(f"🛰️ Fetching TLEs from {TLE_URL}...")
    try:
        response = requests.get(TLE_URL, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"Network error: {e}")
        return

    lines = response.text.strip().splitlines()
    fetched_at = datetime.datetime.utcnow()
    records = []

    for i in range(0, len(lines), 3):
        if i + 2 < len(lines):
            name = lines[i].strip()
            l1 = lines[i+1].strip()
            l2 = lines[i+2].strip()
            parsed = parse_tle_pair(l1, l2, name, fetched_at)
            if parsed:
                records.append(parsed)

    if not records:
        print("No valid TLE records found.")
        return

    df = pd.DataFrame(records)
    print(f"Parsed {len(df)} records. Checking for new data...")

    # A. Update Satellites Table
    with engine.connect() as conn:
        existing_ids = pd.read_sql("SELECT norad_id FROM dim_satellites", conn)
    
    new_sats = df[~df['norad_id'].isin(existing_ids['norad_id'])]
    unique_new_sats = new_sats[['norad_id', 'sat_name', 'intl_designator']].drop_duplicates(subset=['norad_id'])

    if not unique_new_sats.empty:
        print(f"Found {len(unique_new_sats)} new satellites. Saving...")
        unique_new_sats.to_sql('dim_satellites', engine, if_exists='append', index=False)

    # B. Update Telemetry Table
    fact_telem = df[[
        'norad_id', 'epoch_utc', 'fetched_at_utc', 'inclination', 
        'raan', 'eccentricity', 'arg_perigee', 'mean_anomaly', 
        'mean_motion', 'b_star_drag', 'rev_number'
    ]]

    try:
        query = text("SELECT norad_id, epoch_utc FROM fact_telemetry WHERE epoch_utc > NOW() - INTERVAL '3 days'")
        with engine.connect() as conn:
            recent_data = pd.read_sql(query, conn)
        
        # [FIX 3] The Critical "Underscore" Fix
        # We ensure BOTH sides have the underscore separator so the keys match perfectly.
        recent_data['key'] = recent_data['norad_id'].astype(str) + "_" + pd.to_datetime(recent_data['epoch_utc']).astype(str)
        fact_telem['key'] = fact_telem['norad_id'].astype(str) + "_" + pd.to_datetime(fact_telem['epoch_utc']).astype(str)
        
        new_telemetry = fact_telem[~fact_telem['key'].isin(recent_data['key'])].copy()
        new_telemetry = new_telemetry.drop(columns=['key'])
        
        if not new_telemetry.empty:
            print(f"Saving {len(new_telemetry)} new telemetry records...")
            new_telemetry.to_sql('fact_telemetry', engine, if_exists='append', index=False, chunksize=1000)
            print("✅ Telemetry Saved!")
        else:
            print("No new telemetry data (all duplicates).")

    except Exception as e:
        print(f"⚠️ Save Error: {e}")

if __name__ == "__main__":
    main()