import requests
import csv
import datetime
import os

# --- CONFIGURATION ---
URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"
CSV_FILE = "starlink_training_data.csv"

# --- HELPER: TLE PARSER ---
# TLEs are "Fixed Width" text. We slice specific positions to get the numbers.
def parse_tle_pair(line1, line2, sat_name, fetched_at):
    """
    Parses a TLE pair into a dictionary of features for ML.
    Reference: https://celestrak.org/NORAD/documentation/tle-fmt.php
    """
    try:
        # --- LINE 1 EXTRACTION ---
        norad_id = int(line1[2:7])
        classification = line1[7]
        int_designator = line1[9:17].strip()
        epoch_year = int(line1[18:20])
        epoch_day = float(line1[20:32])
        
        # Handle Y2K: TLEs use 2-digit years. 57-99 is 19xx, 00-56 is 20xx
        full_year = 2000 + epoch_year if epoch_year < 57 else 1900 + epoch_year
        
        # Calculate actual Epoch timestamp (Crucial for Time Series)
        epoch_date = datetime.datetime(full_year, 1, 1) + datetime.timedelta(days=epoch_day - 1)
        
        # ML Features from Line 1 (Drag & derivatives)
        ballistic_coeff = line1[33:43].strip() # First Derivative of Mean Motion
        second_deriv = line1[44:52].strip()    # Second Derivative
        bstar = line1[53:61].strip()           # B* Drag Term (Crucial for decay prediction)

        # --- LINE 2 EXTRACTION ---
        inclination = float(line2[8:16])
        raan = float(line2[17:25])             # Right Ascension of Ascending Node
        eccentricity = float("0." + line2[26:33]) # Note: decimal point is implied
        arg_perigee = float(line2[34:42])
        mean_anomaly = float(line2[43:51])
        mean_motion = float(line2[52:63])      # Revs per day
        rev_number = int(line2[63:68])

        return {
            'NORAD_ID': norad_id,
            'Satellite_Name': sat_name,
            'Epoch_UTC': epoch_date.isoformat(), # The PHYSICS time
            'Fetched_At_UTC': fetched_at,        # The CAPTURE time
            'Inclination_deg': inclination,
            'RAAN_deg': raan,
            'Eccentricity': eccentricity,
            'Arg_Perigee_deg': arg_perigee,
            'Mean_Anomaly_deg': mean_anomaly,
            'Mean_Motion_revs_day': mean_motion,
            'B_Star_Drag': bstar,
            'Rev_Number': rev_number,
            # We still keep raw lines just in case
            'Raw_Line1': line1,
            'Raw_Line2': line2
        }
    except Exception as e:
        print(f"Error parsing TLE for {sat_name}: {e}")
        return None

def fetch_and_process_tle():
    print(f"Fetching data from {URL}...")
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Network error: {e}")
        return

    lines = response.text.strip().splitlines()
    fetched_at = datetime.datetime.utcnow().isoformat()
    
    # Identify headers for CSV based on our dictionary keys
    # We do a dummy parse to get keys, or define them explicitly
    headers = [
        'NORAD_ID', 'Satellite_Name', 'Epoch_UTC', 'Fetched_At_UTC',
        'Inclination_deg', 'RAAN_deg', 'Eccentricity', 'Arg_Perigee_deg', 
        'Mean_Anomaly_deg', 'Mean_Motion_revs_day', 'B_Star_Drag', 
        'Rev_Number', 'Raw_Line1', 'Raw_Line2'
    ]

    file_exists = os.path.isfile(CSV_FILE)
    
    # Buffer for valid records
    records = []
    
    # Process in groups of 3 (Name, L1, L2)
    for i in range(0, len(lines), 3):
        if i + 2 < len(lines):
            name = lines[i].strip()
            l1 = lines[i+1].strip()
            l2 = lines[i+2].strip()
            
            parsed_data = parse_tle_pair(l1, l2, name, fetched_at)
            if parsed_data:
                records.append(parsed_data)

    if not records:
        print("No valid records found.")
        return

    # Append to CSV
    new_rows = 0
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        
        if not file_exists:
            writer.writeheader()
            
        for record in records:
            writer.writerow(record)
            new_rows += 1

    print(f"Successfully processed and appended {new_rows} satellite records.")

if __name__ == "__main__":
    fetch_and_process_tle()