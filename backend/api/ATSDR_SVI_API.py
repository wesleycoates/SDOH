# complete_svi_pull.py
import requests
import sqlite3
import json
import time
from db_loader import SDOHDatabaseLoader

def fetch_all_svi_data():
    """
    Fetch all SVI data across the US.
    
    This function pulls SVI data state by state to avoid hitting API limits.
    """
    # List of state FIPS codes (01-56, excluding certain codes)
    state_fips_codes = [
        '01', '02', '04', '05', '06', '08', '09', '10', '11', '12', 
        '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', 
        '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', 
        '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', 
        '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'
    ]
    
    loader = SDOHDatabaseLoader()
    total_records = 0
    
    for state_fips in state_fips_codes:
        print(f"\nFetching SVI data for state FIPS {state_fips}...")
        
        url = "https://onemap.cdc.gov/OneMapServices/rest/services/SVI/CDC_ATSDR_Social_Vulnerability_Index_2020_USA/MapServer/2/query"
        
        # No record limit to get all data for the state
        params = {
            'where': f"STATE='{state_fips}'",
            'outFields': '*',
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                feature_count = len(data.get('features', []))
                print(f"Retrieved {feature_count} census tracts for state {state_fips}")
                
                # Only process if we got data
                if feature_count > 0:
                    loader.load_svi_data(json_data=data)
                    total_records += feature_count
                
                # Add a short delay to be nice to the API
                time.sleep(2)
            else:
                print(f"API request failed for state {state_fips} with status code {response.status_code}")
        except Exception as e:
            print(f"Error processing state {state_fips}: {e}")
    
    print(f"\nCompleted SVI data pull. Total records added: {total_records}")
    
    # Get database stats
    stats = loader.get_database_stats()
    print("\nFinal Database Statistics:")
    for key, value in stats.items():
        if key != 'last_updated':
            print(f"  {key}: {value}")

if __name__ == "__main__":
    fetch_all_svi_data()