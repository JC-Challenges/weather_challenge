# Load in our created definitions
from weather_utils import (
    convert_temp_to_celsius,
    convert_precip_to_cm,
    parse_weather_line,
    extract_station_id,
    extract_year,
    load_weather_file
) 
import sqlite3

# Tests for proper temperature conversions
def test_temperature_conversion():
    raw_value = 100
    result = convert_temp_to_celsius(raw_value)
    assert result == 10

def test_temperature_conversion_negative():
    raw_value = -100
    result = convert_temp_to_celsius(raw_value)
    assert result == -10

def test_temperature_conversion_zero():
    result = convert_temp_to_celsius(0)
    assert result == 0

# Tests for parsing weather txt files line by line    
def test_parse_weather_line_valid():
    line = "19850101\t-6\t-83\t160"
    result = parse_weather_line(line)
    assert result == ("19850101", "-6", "-83", "160")

def test_parse_weather_line_empty():
    result = parse_weather_line("")
    assert result is None

def test_parse_weather_line_whitespace_only():
    result = parse_weather_line(r"   \t  ")
    assert result is None

def test_parse_weather_line_missing_column():
    line = "19850101\t-6\t-83"  # Only 3 values instead of 4
    result = parse_weather_line(line)
    assert result is None

def test_parse_weather_line_with_leading_whitespace():
    line = "  19850101\t-6\t-83\t160  "
    result = parse_weather_line(line)
    assert result == ("19850101", "-6", "-83", "160")

# Tests for station and year extraction from filename/string
def test_extract_station_id():
    result = extract_station_id("USC00110072.txt")
    assert result == "USC00110072"

def test_extract_year():
    assert extract_year(19850101) == 1985
    assert extract_year(20141231) == 2014

# Tests if missing values are incorrect
def test_missing_value_detection():
    missing_value = -9999
    assert missing_value == -9999

# Probably not needed for this, but good if in polar regions
def test_missing_value_not_in_valid_range():
    missing_value = -9999
    reasonable_min = -1000  # -100°C in tenths
    assert missing_value < reasonable_min

# More complex: Testing if loading a file inserts records into the db
def test_load_weather_file_creates_records():
    # Setup as done in main.py
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE weather (
            station TEXT,
            date DATE,
            max_temp INTEGER,
            min_temp INTEGER,
            precipitation INTEGER,
            PRIMARY KEY (station, date)
        )
    ''')
    
    # Create a temporary test file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        # Test data mimicking what is in a weather data file
        f.write("19850101\t-6\t-83\t160\n")
        f.write("19850102\t-50\t-206\t0\n")
        test_filepath = f.name
    
    # Commit to db
    load_weather_file(test_filepath, cur)
    conn.commit()
    
    # Make sure there are two records
    cur.execute("SELECT COUNT(*) FROM weather")
    count = cur.fetchone()[0]
    assert count == 2
    
    # Cleanup temp files and close db connection
    import os
    os.unlink(test_filepath)
    conn.close()
