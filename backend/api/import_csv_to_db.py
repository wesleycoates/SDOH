import os
import pandas as pd
import sqlite3
import glob

def import_csv_to_sqlite(csv_file, db_file):
    """
    Import data from a CSV file into a SQLite database.
    
    Args:
        csv_file (str): Path to the CSV file
        db_file (str): Path to the SQLite database
    """
    print(f"Processing {csv_file} -> {db_file}")
    
    # Extract base name without extension to use as table name
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        print(f"  Read {len(df)} rows from {csv_file}")
        
        # Connect to the database
        conn = sqlite3.connect(db_file)
        
        # Write the data to a SQLite table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"  Successfully imported data into {table_name} table in {db_file}")
        
        # Close the connection
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error importing {csv_file} to {db_file}: {e}")
        return False

def main():
    # Define the mapping of CSV files to database files
    # Map the specific CSV file names to their corresponding databases
    mapping = {
        'SVI_2022_US_ZCTA': 'svi_2022_zip.db',
        'SVI_2022_US': 'svi_2022_us.db',
        'SVI_2022_US_county': 'svi_2022_county.db'
    }
    
    # Get all CSV files in the current directory
    csv_files = glob.glob('*.csv')
    print(f"Found {len(csv_files)} CSV files in the current directory")
    
    if not csv_files:
        print("No CSV files found in the current directory")
        return
    
    successful_imports = 0
    
    # Process each CSV file
    for csv_file in csv_files:
        # Extract base name without extension to match with database
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        
        # Find matching database based on name prefix
        matched_db = None
        for prefix, db_file in mapping.items():
            if base_name.startswith(prefix):
                matched_db = db_file
                break
        
        if matched_db and os.path.exists(matched_db):
            success = import_csv_to_sqlite(csv_file, matched_db)
            if success:
                successful_imports += 1
                # Remove the CSV file after successful import
                os.remove(csv_file)
                print(f"  Deleted {csv_file}")
        else:
            print(f"Could not find matching database for {csv_file}")
    
    print(f"Import complete! Successfully imported {successful_imports} CSV files.")

if __name__ == "__main__":
    main()