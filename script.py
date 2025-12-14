import requests
import csv
import datetime
import os

URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"

CSV_FILE = "starlink_tle_history.csv"

def fetch_and_save_tle():
    print(f"Fetching data from {URL}...")
    response = requests.get(URL)

    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return

    lines = response.text.strip().splitlines()

    # Use UTC timestamp (recommended)
    fetched_at = datetime.datetime.utcnow().isoformat()

    # Check if file exists (to decide header)
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header only once
        if not file_exists:
            writer.writerow([
                'Satellite_Name',
                'Line_1',
                'Line_2',
                'Fetched_At'
            ])

        count = 0
        for i in range(0, len(lines), 3):
            if i + 2 < len(lines):
                writer.writerow([
                    lines[i].strip(),
                    lines[i + 1].strip(),
                    lines[i + 2].strip(),
                    fetched_at
                ])
                count += 1

    print(f"Appended {count} satellites at {fetched_at}")

if __name__ == "__main__":
    fetch_and_save_tle()
