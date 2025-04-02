import requests
import sqlite3
import json

# Create SQLite database
conn = sqlite3.connect('social_determinants.db')
cursor = conn.cursor()

# Create a table for SVI data
cursor.execute('''
CREATE TABLE IF NOT EXISTS svi_data (
    FIPS TEXT PRIMARY KEY,
    STATE TEXT,
    COUNTY TEXT,
    LOCATION TEXT,
    OVERALL_SVI REAL,
    THEME1_SVI REAL,
    THEME2_SVI REAL,
    THEME3_SVI REAL,
    THEME4_SVI REAL
)
''')

# Query the SVI API for census tract data (layer 2)
url = "https://onemap.cdc.gov/OneMapServices/rest/services/SVI/CDC_ATSDR_Social_Vulnerability_Index_2020_USA/MapServer/2/query"
params = {
    'where': '1=1',  # Get all records
    'outFields': 'FIPS,STATE,COUNTY,LOCATION,RPL_THEMES,RPL_THEME1,RPL_THEME2,RPL_THEME3,RPL_THEME4',
    'returnGeometry': 'false',
    'f': 'json',
    'resultRecordCount': 100  # Limit to 100 records for testing
}

response = requests.get(url, params=params)
data = response.json()

# Insert data into SQLite
for feature in data['features']:
    attr = feature['attributes']
    cursor.execute('''
    INSERT OR REPLACE INTO svi_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        attr['FIPS'],
        attr['STATE'],
        attr['COUNTY'],
        attr['LOCATION'],
        attr.get('RPL_THEMES'),
        attr.get('RPL_THEME1'),
        attr.get('RPL_THEME2'),
        attr.get('RPL_THEME3'),
        attr.get('RPL_THEME4')
    ))

conn.commit()
conn.close()
print(f"Saved {len(data['features'])} SVI records to database")