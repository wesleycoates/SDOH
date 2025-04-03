# db_setup_fresh.py
import sqlite3
import os
import datetime

def setup_fresh_database(db_path='social_determinants.db', delete_existing=True):
    """
    Create a fresh SQLite database with tables for social determinants of health data.
    
    Parameters:
    -----------
    db_path : str
        Path to the SQLite database file
    delete_existing : bool
        If True, delete existing tables before creating new ones
    """
    # Check if database exists and create directory if needed
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get existing tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = [row[0] for row in cursor.fetchall()]
    print(f"Existing tables: {existing_tables}")
    
    # Delete existing tables if requested
    if delete_existing and existing_tables:
        # Skip SQLite internal tables
        tables_to_drop = [t for t in existing_tables if not t.startswith('sqlite_')]
        
        for table in tables_to_drop:
            print(f"Dropping table: {table}")
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Create fresh tables
    
    # 1. Social Vulnerability Index (SVI) table
    print("Creating SVI table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS svi_data (
        fips TEXT PRIMARY KEY,
        state TEXT,
        county TEXT,
        location TEXT,
        overall_svi REAL,
        socioeconomic_svi REAL,
        household_svi REAL,
        minority_svi REAL,
        housing_transport_svi REAL,
        last_updated TEXT
    )
    ''')
    
    # 2. Area Deprivation Index (ADI) table
    print("Creating ADI table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS adi_data (
        block_group_id TEXT PRIMARY KEY,
        state TEXT,
        county TEXT,
        adi_national_rank INTEGER,
        adi_state_rank INTEGER,
        adi_national_decile INTEGER,
        last_updated TEXT
    )
    ''')
    
    # 3. CDC PLACES health data
    print("Creating PLACES table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS places_data (
        location_id TEXT,
        location_type TEXT,
        measure_id TEXT,
        measure TEXT,
        data_value REAL,
        confidence_limit_low REAL,
        confidence_limit_high REAL,
        year TEXT,
        last_updated TEXT,
        PRIMARY KEY (location_id, measure_id)
    )
    ''')
    
    # 4. User locations table for app functionality
    print("Creating user_locations table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        location_name TEXT,
        address TEXT,
        latitude REAL,
        longitude REAL,
        census_tract TEXT,
        block_group TEXT,
        zcta TEXT,
        created_date TEXT,
        FOREIGN KEY (census_tract) REFERENCES svi_data(fips),
        FOREIGN KEY (block_group) REFERENCES adi_data(block_group_id)
    )
    ''')
    
    # 5. Create table to track data sources and last update times
    print("Creating data_sources table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS data_sources (
        source_name TEXT PRIMARY KEY,
        source_url TEXT,
        last_updated TEXT,
        update_frequency TEXT,
        description TEXT
    )
    ''')
    
    # Create indexes for faster querying
    print("Creating indexes...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_svi_state ON svi_data(state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_svi_county ON svi_data(county)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_adi_state ON adi_data(state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_adi_county ON adi_data(county)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_places_measure_id ON places_data(measure_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_places_location_type ON places_data(location_type)')
    
    # Insert initial data source information
    initial_sources = [
        ('CDC_SVI', 'https://onemap.cdc.gov/OneMapServices/rest/services/SVI/CDC_ATSDR_Social_Vulnerability_Index_2020_USA/MapServer', 
         None, 'Annual', 'CDC/ATSDR Social Vulnerability Index data'),
        ('ADI', 'https://www.neighborhoodatlas.medicine.wisc.edu/', 
         None, 'Variable', 'Area Deprivation Index from Neighborhood Atlas'),
        ('CDC_PLACES', 'https://data.cdc.gov/api', 
         None, 'Annual', 'CDC PLACES health indicators')
    ]
    
    cursor.executemany('''
    INSERT OR REPLACE INTO data_sources (source_name, source_url, last_updated, update_frequency, description)
    VALUES (?, ?, ?, ?, ?)
    ''', initial_sources)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print(f"Database setup complete at: {db_path}")
    print("The following tables were created:")
    print("- svi_data: Social Vulnerability Index data")
    print("- adi_data: Area Deprivation Index data")
    print("- places_data: CDC PLACES health indicators")
    print("- user_locations: User-specific location data")
    print("- data_sources: Metadata about data sources")

if __name__ == "__main__":
    setup_fresh_database()