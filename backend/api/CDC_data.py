import requests
import sqlite3
import pandas as pd
from sodapy import Socrata

# Create SQLite database connection
conn = sqlite3.connect('social_determinants.db')

# CDC PLACES data uses Socrata Open Data API
client = Socrata("data.cdc.gov", None)  # No authentication for public data

# Example: Get census tract data for a specific measure (smoking)
results = client.get("cwsq-ngmh", limit=1000, 
                     select="locationname, locationid, data_value, measure",
                     where="measureid='CSMOKING'")

# Convert to pandas DataFrame
df = pd.DataFrame.from_records(results)

# Save to SQLite
df.to_sql('places_smoking_data', conn, if_exists='replace', index=False)

print(f"Saved {len(df)} PLACES records to database")
conn.close()