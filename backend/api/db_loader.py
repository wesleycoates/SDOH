# db_loader.py
import sqlite3
import datetime
import requests
import json
import pandas as pd
import os

class SDOHDatabaseLoader:
    """
    Class to handle loading of social determinants of health data into SQLite database.
    """
    
    def __init__(self, db_path='social_determinants.db'):
        """Initialize with path to SQLite database."""
        self.db_path = db_path
        self._check_database()
    
    def _check_database(self):
        """Check if the database exists and has required tables."""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}. Run db_setup_fresh.py first.")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check for required tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['svi_data', 'places_data', 'data_sources']
        for table in required_tables:
            if table not in tables:
                conn.close()
                raise ValueError(f"Required table '{table}' not found in database. Run db_setup_fresh.py first.")
        
        conn.close()
    
    def load_svi_data(self, json_data=None, csv_path=None, api_response=None):
        """
        Load SVI data into the database from various sources.
        
        Parameters:
        -----------
        json_data : dict
            Preprocessed JSON data
        csv_path : str
            Path to CSV file with SVI data
        api_response : requests.Response
            Direct response from API call
        """
        # Process the API response if provided
        if api_response is not None:
            if api_response.status_code != 200:
                raise ValueError(f"API request failed with status code {api_response.status_code}")
            json_data = api_response.json()
        
        # Ensure we have some data to process
        if json_data is None and csv_path is None:
            raise ValueError("Must provide either json_data, csv_path, or api_response")
        
        conn = sqlite3.connect(self.db_path)
        current_time = datetime.datetime.now().isoformat()
        
        if json_data:
            # Process the JSON data
            records = []
            features = json_data.get('features', [])
            
            for feature in features:
                attr = feature.get('attributes', {})
                records.append({
                    'fips': attr.get('FIPS'),
                    'state': attr.get('STATE'),
                    'county': attr.get('COUNTY'),
                    'location': attr.get('LOCATION'),
                    'overall_svi': attr.get('RPL_THEMES'),
                    'socioeconomic_svi': attr.get('RPL_THEME1'),
                    'household_svi': attr.get('RPL_THEME2'),
                    'minority_svi': attr.get('RPL_THEME3'),
                    'housing_transport_svi': attr.get('RPL_THEME4'),
                    'last_updated': current_time
                })
            
            df = pd.DataFrame(records)
            
        elif csv_path:
            # Read from CSV
            df = pd.read_csv(csv_path)
            
            # Map columns if needed
            column_mapping = {
                # Add mappings here if column names differ
                # 'csv_column': 'db_column'
            }
            
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)
            
            df['last_updated'] = current_time
        
        # Save to database
        if not df.empty:
            df.to_sql('svi_data', conn, if_exists='append', index=False)
            
            # Update data_sources
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE data_sources SET last_updated = ? WHERE source_name = 'CDC_SVI'", 
                (current_time,)
            )
            
            conn.commit()
            print(f"Successfully loaded {len(df)} SVI records into database")
        else:
            print("No SVI records to load")
        
        conn.close()
    
    def load_places_data(self, json_data=None, csv_path=None, api_response=None):
        """
        Load CDC PLACES data into the database from various sources.
        
        Parameters:
        -----------
        json_data : dict or list
            Preprocessed JSON data
        csv_path : str
            Path to CSV file with PLACES data
        api_response : requests.Response
            Direct response from API call
        """
        # Process the API response if provided
        if api_response is not None:
            if api_response.status_code != 200:
                raise ValueError(f"API request failed with status code {api_response.status_code}")
            json_data = api_response.json()
        
        # Ensure we have some data to process
        if json_data is None and csv_path is None:
            raise ValueError("Must provide either json_data, csv_path, or api_response")
        
        conn = sqlite3.connect(self.db_path)
        current_time = datetime.datetime.now().isoformat()
        
        if json_data:
            # Process JSON data
            records = []
            
            # Handle different JSON structures
            if isinstance(json_data, list):
                data_items = json_data
            else:
                data_items = json_data.get('results', [])
            
            for item in data_items:
                record = {
                    'location_id': item.get('locationid'),
                    'location_type': item.get('locationtype'),
                    'measure_id': item.get('measureid'),
                    'measure': item.get('measure'),
                    'data_value': item.get('data_value'),
                    'confidence_limit_low': item.get('low_confidence_limit'),
                    'confidence_limit_high': item.get('high_confidence_limit'),
                    'year': item.get('year'),
                    'last_updated': current_time
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            
        elif csv_path:
            # Read from CSV
            df = pd.read_csv(csv_path)
            
            # Map columns if needed
            column_mapping = {
                # Add mappings here if column names differ
                # 'csv_column': 'db_column'
            }
            
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)
            
            df['last_updated'] = current_time
        
        # Save to database
        if not df.empty:
            df.to_sql('places_data', conn, if_exists='append', index=False)
            
            # Update data_sources
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE data_sources SET last_updated = ? WHERE source_name = 'CDC_PLACES'", 
                (current_time,)
            )
            
            conn.commit()
            print(f"Successfully loaded {len(df)} PLACES records into database")
        else:
            print("No PLACES records to load")
        
        conn.close()
    
    def query_location_data(self, location_id, location_type='tract'):
        """Query data for a specific location."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        result = {
            'location_id': location_id,
            'location_type': location_type,
            'svi_data': None,
            'places_data': []
        }
        
        # Query SVI data if tract-level
        if location_type == 'tract':
            cursor.execute("SELECT * FROM svi_data WHERE fips = ?", (location_id,))
            row = cursor.fetchone()
            if row:
                result['svi_data'] = dict(row)
        
        # Query PLACES data
        cursor.execute(
            "SELECT * FROM places_data WHERE location_id = ? AND location_type = ?", 
            (location_id, location_type)
        )
        rows = cursor.fetchall()
        if rows:
            result['places_data'] = [dict(row) for row in rows]
        
        conn.close()
        return result
    
    def get_database_stats(self):
        """Get statistics about the database contents."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # Get table counts
        tables = ['svi_data', 'adi_data', 'places_data', 'user_locations', 'data_sources']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            stats[f"{table}_count"] = count
        
        # Get last updated times
        cursor.execute("SELECT source_name, last_updated FROM data_sources")
        updates = cursor.fetchall()
        stats['last_updated'] = {name: updated for name, updated in updates}
        
        conn.close()
        return stats


# Example usage
if __name__ == "__main__":
    loader = SDOHDatabaseLoader()
    
    # Print database stats
    stats = loader.get_database_stats()
    print("Database Statistics:")
    for key, value in stats.items():
        if key != 'last_updated':
            print(f"  {key}: {value}")
    
    print("\nLast Updated Times:")
    for source, time in stats.get('last_updated', {}).items():
        print(f"  {source}: {time if time else 'Never'}")