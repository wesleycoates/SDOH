import sqlite3
import pandas as pd

def copy_table_between_databases(source_db, target_db, table_name):
    """
    Copy a table from source database to target database and delete from source.
    
    Args:
        source_db (str): Path to the source SQLite database
        target_db (str): Path to the target SQLite database
        table_name (str): Name of the table to copy
    """
    print(f"Copying table {table_name} from {source_db} to {target_db}")
    
    try:
        # Connect to source database
        source_conn = sqlite3.connect(source_db)
        
        # Load the data from source database
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, source_conn)
        print(f"  Read {len(df)} rows from {table_name} in {source_db}")
        
        # Connect to target database
        target_conn = sqlite3.connect(target_db)
        
        # Write the data to the target database
        df.to_sql(table_name, target_conn, if_exists='replace', index=False)
        print(f"  Successfully copied {table_name} to {target_db}")
        
        # Drop the table from the source database
        cursor = source_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        source_conn.commit()
        print(f"  Deleted {table_name} from {source_db}")
        
        # Close connections
        source_conn.close()
        target_conn.close()
        
        return True
    except Exception as e:
        print(f"Error copying table: {e}")
        return False

def main():
    # Define the source and target databases and table name
    source_db = "svi_2022_us.db"
    target_db = "svi_2022_county.db"
    table_name = "SVI_2022_US_county"
    
    # Copy the table and delete the original
    success = copy_table_between_databases(source_db, target_db, table_name)
    
    if success:
        print("Operation completed successfully!")
    else:
        print("Operation failed.")

if __name__ == "__main__":
    main()