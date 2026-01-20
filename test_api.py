import pytest
from api import app

@pytest.fixture
# Creates a test client for Flask
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

# Tests the home endpoint returns correct message
def test_home_endpoint(client):
    response = client.get('/')
    assert response.status_code == 200
    assert response.json['message'] == 'Weather API is running'

# Tests data is returned for the weather endpoint
def test_weather_endpoint_returns_data(client):
    response = client.get('/api/weather')
    assert response.status_code == 200
    assert 'data' in response.json
    assert 'pagination' in response.json

# Tests the pagination endpoints
def test_weather_endpoint_pagination(client):
    response = client.get('/api/weather?page=1&per_page=10')
    assert response.status_code == 200
    assert response.json['pagination']['per_page'] == 10
    assert len(response.json['data']) <= 10

# Tests filtering by station
def test_weather_endpoint_station_filter(client):
    response = client.get('/api/weather?station=USC00110072')
    assert response.status_code == 200
    for record in response.json['data']:
        assert record['station'] == 'USC00110072'

# Tests data is returned for the weather stats endpoint
def test_weather_stats_endpoint_returns_data(client):
    """Test the stats endpoint returns data."""
    response = client.get('/api/weather/stats')
    assert response.status_code == 200
    assert 'data' in response.json
    assert 'pagination' in response.json

# Tests filtering by year
def test_weather_stats_endpoint_year_filter(client):
    """Test filtering by year works."""
    response = client.get('/api/weather/stats?year=1985')
    assert response.status_code == 200
    for record in response.json['data']:
        assert record['year'] == 1985

# Tests Swagger documentation is available
def test_swagger_endpoint_available(client):
    response = client.get('/apidocs/')
    assert response.status_code == 200