import os
from pathlib import Path
from tqdm import tqdm
import time

# Timer class and function
class Timer:
    def __init__(self):
        self.start_time = None
        
    def start(self):
        self.start_time = time.time()
        
    def stop(self):
        if self.start_time is None:
            raise ValueError("Timer not started")
        elapsed_time = time.time() - self.start_time
        self.start_time = None
        return elapsed_time
        
# Simple temp conversion
def convert_temp_to_celsius(tenths_of_celsius):
    return tenths_of_celsius / 10
    
# Simple precip conversion
def convert_precip_to_cm(tenths_of_mm):
    return tenths_of_mm / 100
    
# Station integer extraction from filename
def extract_station_id(filename):
    return filename.replace('.txt', '')

# Extracts year from YYYYMMDD date format
def extract_year(date_int):
    return date_int // 10000

# Parse the tab separated entries into single components
def parse_weather_line(line):
    line = line.strip()
    if not line:
        return None
    try:
        date, max_temp, min_temp, precip = line.split('\t', 3)
        return (date, max_temp, min_temp, precip)
    except ValueError:
        return None

# Load one single file into db
def load_weather_file(filepath, cursor):
    filename = os.path.basename(filepath)
    station_id = extract_station_id(filename)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            parsed = parse_weather_line(line)
            if parsed is None:
                continue          
            date, max_temp, min_temp, precip = parsed
            cursor.execute(
                'INSERT OR IGNORE INTO weather (station, date, max_temp, min_temp, precipitation) VALUES (?, ?, ?, ?, ?)',
                (station_id, date, max_temp, min_temp, precip)
            )

# Loads ALL txt weather files in a directory
def load_all_weather_files(directory, cursor):
    for filename in tqdm(os.listdir(directory), desc="Processing station data files..."):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath) and filename.endswith('.txt'):
            load_weather_file(filepath, cursor)