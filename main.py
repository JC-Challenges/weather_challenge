# coding: utf-8

"""
This script ingests weather and crop yield data, calculating averages and sums as needed, loads them into SQLite databases and then launches a REST API for viewing and filtering the data. See README and requirements files for further information.
Author: Jake Campbell
Created: 01-20-2026
"""

import sqlite3
import os
from pathlib import Path
import time
from tqdm import tqdm
import pandas as pd
import numpy as np
from weather_utils import load_all_weather_files, Timer

# Table creation definitions (3)
def create_weather_table(cursor):
    """Create the weather table if it doesn't exist."""
    cursor.execute('DROP TABLE IF EXISTS weather')
    cursor.execute('''
        CREATE TABLE weather (
            station TEXT,
            date DATE,
            max_temp INTEGER,
            min_temp INTEGER,
            precipitation INTEGER,
            PRIMARY KEY (station, date)
        )
    ''')
def create_yearly_table(cursor):
    """Create yearly weather table if it doesn't exist"""
    cursor.execute('DROP TABLE IF EXISTS weather_yearly')
    cursor.execute('''
        CREATE TABLE weather_yearly (
            station TEXT,
            year INTEGER,
            avg_max_temp_degC REAL,
            avg_min_temp_degC REAL,
            total_precipitation_cm REAL,
            PRIMARY KEY (station, year)
        )
    ''')

def create_yield_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS crop_yields')
    cursor.execute('''
        CREATE TABLE crop_yields (
            year INTEGER PRIMARY KEY,
            yield_bushels INTEGER
        )
    ''')

def main():
    # Configuration
    db_path = 'weather.db'
    data_directory = 'wx_data'
    yld_filepath  = 'yld_data/US_corn_grain_yield.txt'

    # Timer function
    t = Timer()
    t.start()
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Set up tables
    create_weather_table(cur)
    create_yearly_table(cur)
    create_yield_table(cur)
    
    # Load data
    load_all_weather_files(data_directory, cur)

    # Read in SQL table into pandas for yearly calculations
    df = pd.read_sql("SELECT * FROM weather", conn)
    df = df.replace(-9999, np.nan) # Ignores missing data
    df['year'] = df['date'] // 10000 # Removes month/day
    
    ################################### Stats Calcs ##########################################
    
    # Group by year for average and sum calcs
    grouped_df = df.groupby(['station', 'year']).agg(
    avg_max_temp = ('max_temp', 'mean'),
    avg_min_temp = ('min_temp', 'mean'),
    total_precipitation = ('precipitation', 'sum')).reset_index()
    
    # Unit conversions for all variables
    grouped_df['avg_max_temp_degC'] = (grouped_df['avg_max_temp'] / 10).round(2)
    grouped_df['avg_min_temp_degC'] = (grouped_df['avg_min_temp'] / 10).round(2)
    grouped_df['total_precipitation_cm'] = grouped_df['total_precipitation'] / 100
    
    # Drop old column data
    grouped_df.drop(columns=['avg_max_temp', 'avg_min_temp', 'total_precipitation'], inplace = True)
    
    # Port Pandas DF to SQL
    grouped_df.to_sql('weather_yearly', conn, if_exists='append', index=False)
    
    ################################### Stats Calcs ##########################################

    ################################### Yield Data ###########################################
    
    with open(yld_filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            try:
                # Split line by tab and extract 4 values
                col1, col2 = line.split('\t', 3)  # Split on first 3 tabs
                # Insert into database
                cur.execute(
                    'INSERT OR IGNORE INTO crop_yields (year, yield_bushels) VALUES (?, ?)',
                    (col1, col2)
                )
            except ValueError as e:
                print(f"Line {line_num} in {yld_filepath} has incorrect format: {line}")
                continue  # Skip bad lines
                
    ################################### Yield Data ###########################################
    
    # Commit db
    conn.commit()

    # Timer completion
    print("Data successfully imported into weather, yearly, and yield SQLite tables.")
    elapsed = t.stop()
    print(f"Elapsed processing time: {elapsed:.6f} seconds")
    
    # To see how many records are in each table
    cur.execute('SELECT COUNT(*) FROM weather')
    count0 = cur.fetchone()[0]
    print(f"\nTotal records in the weather station table: {count0}")

    cur.execute('SELECT COUNT(*) FROM weather_yearly')
    count1 = cur.fetchone()[0]
    print(f"\nTotal records in the yearly table: {count1}")

    cur.execute('SELECT COUNT(*) FROM crop_yields')
    count2 = cur.fetchone()[0]
    print(f"\nTotal records in the weather station table: {count2}")

    # Close db when done
    conn.close()
    
if __name__ == '__main__':
    main()

