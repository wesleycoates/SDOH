# load_sdoh_data.py
import requests
import json
import sqlite3
import datetime
import sys
from db_loader import SDOHDatabaseLoader

def fetch_svi_data(state_fips=None, county_fips=None, limit=500):
    """
    Fetch SVI data from CDC's API.
    
    Parameters:
    -----------
    state_fips : str
        State FIPS code (e.g., '01' for Alabama)
    county_fips : str
        County FIPS code (must include state, e.g., '01001')
    limit : int
        Maximum number of records to fetch
    
    Returns:
    --------
    requests.Response
        API response object
    """
    print(f"Fetching SVI data...")
    
    url = "https://onemap.cdc.gov/OneMapServices/rest/services/SVI/CDC_ATSDR_Social_Vulnerability_Index_2020_USA/MapServer/2/query"
    
    # Build the where clause based on inputs
    where_clause = "1=1"  # Default to get all
    if state_fips:
        where_clause = f"STATE='{state_fips}'"
    if county_fips:
        where_clause = f"STCNTY='{county_fips}'"
    
    # Fields to retrieve
    fields = [
        'FIPS', 'STATE', 'ST_ABBR', 'STCNTY', 'COUNTY', 'LOCATION',
        'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4'
    ]
    
    params = {
        'where': where_clause,
        'outFields': ','.join(fields),
        'returnGeometry': 'false',
        'f': 'json',
        'resultRecordCount': limit
    }
    
    try:
        print(f"Sending request to SVI API with parameters: {params}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            feature_count = len(data.get('features', []))
            print(f"Successfully retrieved {feature_count} census tracts from SVI API")
        else:
            print(f"SVI API request failed with status code {response.status_code}")
            print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error fetching SVI data: {e}")
        response = None
    
    return response

def fetch_places_data(state_abbr=None, county_fips=None, measures=None, limit=1000):
    """
    Fetch PLACES data from CDC's API.
    
    Parameters:
    -----------
    state_abbr : str
        State abbreviation (e.g., 'AL' for Alabama)
    county_fips : str
        County FIPS code (must include state, e.g., '01001')
    measures : list
        List of measure IDs to fetch (e.g., ['CSMOKING', 'BPHIGH'])
    limit : int
        Maximum number of records to fetch
    
    Returns:
    --------
    requests.Response
        API response object
    """
    if measures is None:
        # Default to these common health measures if none specified
        measures = [
            'CSMOKING',    # Current smoking
            'BPHIGH',      # High blood pressure
            'DEPRESSION',  # Depression
            'OBESITY',     # Obesity
            'DIABETES',    # Diabetes
        ]
    
    print(f"Fetching PLACES data for measures: {', '.join(measures)}")
    
    # Using data.cdc.gov endpoint with Socrata API format
    url = "https://data.cdc.gov/resource/cwsq-ngmh.json"
    
    # Build the where clause based on inputs
    where_clauses = []
    if state_abbr:
        where_clauses.append(f"stateabbr='{state_abbr}'")
    if county_fips:
        where_clauses.append(f"locationid LIKE '{county_fips}%'")
    if measures:
        measure_clause = " OR ".join([f"measureid='{m}'" for m in measures])
        where_clauses.append(f"({measure_clause})")
    
    where_str = " AND ".join(where_clauses) if where_clauses else None
    
    params = {
        '$limit': limit
    }
    
    if where_str:
        params['$where'] = where_str
    
    try:
        print(f"Sending request to PLACES API with parameters: {params}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully retrieved {len(data)} PLACES records")
        else:
            print(f"PLACES API request failed with status code {response.status_code}")
            print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error fetching PLACES data: {e}")
        response = None
    
    return response

def load_all_data(db_path='social_determinants.db', state_fips=None, state_abbr=None):
    """
    Load both SVI and PLACES data for a given state.
    
    Parameters:
    -----------
    db_path : str
        Path to SQLite database
    state_fips : str
        State FIPS code (e.g., '01' for Alabama)
    state_abbr : str
        State abbreviation (e.g., 'AL' for Alabama)
    """
    # Initialize the database loader
    try:
        loader = SDOHDatabaseLoader(db_path)
        print(f"Successfully connected to database at {db_path}")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return
    
    # Get initial database stats
    initial_stats = loader.get_database_stats()
    print("\nInitial Database Statistics:")
    for key, value in initial_stats.items():
        if key != 'last_updated':
            print(f"  {key}: {value}")
    
    # Fetch and load SVI data
    print("\n" + "="*50)
    print("FETCHING AND LOADING SVI DATA")
    print("="*50)
    svi_response = fetch_svi_data(state_fips=state_fips, limit=1000)
    if svi_response and svi_response.status_code == 200:
        try:
            loader.load_svi_data(api_response=svi_response)
            print("SVI data loaded successfully")
        except Exception as e:
            print(f"Error loading SVI data: {e}")
    else:
        print("No SVI data to load")
    
    # Fetch and load PLACES data
    print("\n" + "="*50)
    print("FETCHING AND LOADING PLACES DATA")
    print("="*50)
    # Define a comprehensive set of health measures
    health_measures = [
        'CSMOKING',    # Current smoking
        'BPHIGH',      # High blood pressure
        'DEPRESSION',  # Depression
        'OBESITY',     # Obesity
        'DIABETES',    # Diabetes
        'PHLTH',       # Physical health not good for ≥14 days
        'MHLTH',       # Mental health not good for ≥14 days
        'CHOLSCREEN',  # Cholesterol screening
        'ACCESS2',     # Health insurance
        'COLON_SCREEN',# Colorectal cancer screening
        'MAMMOUSE',    # Mammography use
        'CERVICAL',    # Cervical cancer screening
        'DENTAL',      # Dental visit
        'CHECKUP',     # Annual checkup
        'COREM',       # Core preventive services for men
        'COREW',       # Core preventive services for women
    ]
    
    places_response = fetch_places_data(
        state_abbr=state_abbr, 
        measures=health_measures, 
        limit=5000
    )
    
    if places_response and places_response.status_code == 200:
        try:
            loader.load_places_data(api_response=places_response)
            print("PLACES data loaded successfully")
        except Exception as e:
            print(f"Error loading PLACES data: {e}")
    else:
        print("No PLACES data to load")
    
    # Get updated database stats
    final_stats = loader.get_database_stats()
    print("\n" + "="*50)
    print("FINAL DATABASE STATISTICS")
    print("="*50)
    for key, value in final_stats.items():
        if key != 'last_updated':
            initial = initial_stats.get(key, 0)
            added = value - initial
            print(f"  {key}: {value} (Added: {added})")
    
    print("\nLast Updated Times:")
    for source, time in final_stats.get('last_updated', {}).items():
        print(f"  {source}: {time if time else 'Never'}")

if __name__ == "__main__":
    # Default to Alabama if no state specified
    default_state_fips = '01'
    default_state_abbr = 'AL'
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Load SDOH data into SQLite database')
    parser.add_argument('--db', default='social_determinants.db', help='Path to SQLite database')
    parser.add_argument('--state_fips', default=default_state_fips, help='State FIPS code')
    parser.add_argument('--state_abbr', default=default_state_abbr, help='State abbreviation')
    args = parser.parse_args()
    
    print(f"Loading data for state {args.state_abbr} (FIPS: {args.state_fips}) into {args.db}")
    load_all_data(
        db_path=args.db,
        state_fips=args.state_fips,
        state_abbr=args.state_abbr
    )