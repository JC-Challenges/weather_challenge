from flask import Flask, request, jsonify
from flasgger import Swagger
import sqlite3


app = Flask(__name__)

# Swagger UI configuration setup
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec",
            "route": "/apispec.json",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/"
}

swagger_template = {
    "info": {
        "title": "Weather API",
        "description": "API for weather data and statistics",
        "version": "1.0.0"
    }
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)

# Database configuration
DATABASE = 'weather.db'

# Creates connection to SQLite db
def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # Allows access columns by name
    return conn

# Creates the home API message
@app.route('/')
def home():
    return jsonify({"message": "Weather API is running"})

# Creates the weather data with filtering and pagination
@app.route('/api/weather', methods=['GET'])
def get_weather():
    """
    parameters:
      - name: station
        in: query
        type: string
        required: false
        description: Filter by station ID (e.g., USC00110072)
      - name: date
        in: query
        type: integer
        required: false
        description: Filter by date in YYYYMMDD format (e.g., 19850101)
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Page number
      - name: per_page
        in: query
        type: integer
        required: false
        default: 50
        description: Records per page (max 100)
    responses:
      200:
        description: A list of weather records for stations in Nebraska, Iowa, Illinois, Indiana, or Ohio.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get filter parameters from query string
    station = request.args.get('station')
    date = request.args.get('date')
    
    # Get pagination parameters (with defaults)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # Cap per_page to prevent huge requests
    per_page = min(per_page, 100)
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Build base WHERE clause
    where_clause = '1=1' # Trick to simplify code and use AND, no extra logic needed
    params = []

    # Station/date filtering
    if station:
        where_clause += ' AND station = ?'
        params.append(station)
    
    if date:
        where_clause += ' AND date = ?'
        params.append(date)
    
    # Get total count for pagination metadata
    count_query = f'SELECT COUNT(*) FROM weather WHERE {where_clause}'
    cur.execute(count_query, params)
    total_records = cur.fetchone()[0]
    
    # Get paginated data
    data_query = f'SELECT * FROM weather WHERE {where_clause} LIMIT ? OFFSET ?'
    cur.execute(data_query, params + [per_page, offset])
    rows = cur.fetchall()
    
    # Convert rows to list of dictionaries
    results = []
    for row in rows:
        results.append({
            'station': row['station'],
            'date': row['date'],
            'max_temp': row['max_temp'],
            'min_temp': row['min_temp'],
            'precipitation': row['precipitation']
        })
    
    conn.close()
    
    # Calculate total pages
    total_pages = (total_records + per_page - 1) // per_page

    # Returns JSON-formatted response
    return jsonify({
        'data': results,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_records': total_records,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    })
    
# Return yearly weather statistics with optional filtering and pagination.
@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    """
    parameters:
      - name: station
        in: query
        type: string
        required: false
        description: Filter by station ID (e.g., USC00110072)
      - name: year
        in: query
        type: integer
        required: false
        description: Filter by year (e.g., 1985)
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Page number
      - name: per_page
        in: query
        type: integer
        required: false
        default: 50
        description: Records per page (max 100)
    responses:
      200:
        description: A list of yearly weather statistics for stations in Nebraska, Iowa, Illinois, Indiana, or Ohio.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get filter parameters from query string
    station = request.args.get('station')
    year = request.args.get('year', type=int)
    
    # Get pagination parameters (with defaults)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # Cap per_page to prevent huge requests
    per_page = min(per_page, 100)
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Build base WHERE clause
    where_clause = '1=1'
    params = []

    # Station/Year filtering
    if station:
        where_clause += ' AND station = ?'
        params.append(station)
    
    if year:
        where_clause += ' AND year = ?'
        params.append(year)
    
    # Get total count for pagination metadata
    count_query = f'SELECT COUNT(*) FROM weather_yearly WHERE {where_clause}'
    cur.execute(count_query, params)
    total_records = cur.fetchone()[0]
    
    # Get paginated data
    data_query = f'SELECT * FROM weather_yearly WHERE {where_clause} LIMIT ? OFFSET ?'
    cur.execute(data_query, params + [per_page, offset])
    rows = cur.fetchall()
    
    # Convert rows to list of dictionaries
    results = []
    for row in rows:
        results.append({
            'station': row['station'],
            'year': row['year'],
            'avg_max_temp_degC': row['avg_max_temp_degC'],
            'avg_min_temp_degC': row['avg_min_temp_degC'],
            'total_precipitation_cm': row['total_precipitation_cm']
        })
    
    conn.close()
    
    # Calculate total pages
    total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 0

    # Returns JSON-formatted response
    return jsonify({
        'data': results,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_records': total_records,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    })

# Return yearly US crop yield data with year filtering.
@app.route('/api/weather/yield', methods=['GET'])
def get_yield():
    """
    parameters:
      - name: year
        in: query
        type: integer
        required: false
        description: Filter by year in YYYY format (e.g., 1985)
    responses:
      200:
        description: A list of US crop yield records
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get filter parameters from query string
    year = request.args.get('year')
    
    # Build query
    where_clause = '1=1'
    params = []

    # Year filtering or all years
    if year:
        cur.execute('SELECT * FROM crop_yields WHERE year = ?', [year])
    else:
        cur.execute('SELECT * FROM crop_yields')
    
    rows = cur.fetchall()
    
    # Convert rows to list of dictionaries
    results = []
    for row in rows:
        results.append({
            'year': row['year'],
            'yield_bushels': row['yield_bushels']
        })

    conn.close()
    
    # Returns JSON-formatted response
    return jsonify({
        'data': results,
        'count': len(results)
    })
    
if __name__ == '__main__':
    app.run(debug=True)