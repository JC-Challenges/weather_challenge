# Corteva_coding_assessment
Files for Corteva weather data coding assessment
=======
# Weather Data API - Corteva Coding Exercise

## Overview
This project ingests weather station data and crop yield data (and calculates weather data statistics) into a SQLite database and provides a REST API for accessing the data. The weather data and crop yield data are available at https://github.com/corteva/code-challenge-template. If downloading and running on local machine, make sure raw data files are in same working project directory as python scripts.

## Setup

### Install dependencies with version
```
pip install -r requirements.txt
```

### Load data into SQLite database
```
python main.py
```

### Run the API
```
python api.py
```

The API will be available at http://127.0.0.1:5000

### Run tests
```
pytest test_weather.py test_api.py -v
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/weather` | Weather station data (filterable by station, date) |
| `/api/weather/stats` | Yearly statistics (filterable by station, year) |
| `/api/yield` | US corn yield data (filterable by year) |

API documentation available at `/apidocs/`

## AWS Deployment Approach (Extra Credit)

After some researching and with no AWS experience (only HPC), I believe AWS Elastic Beanstalk would be a good API deployment service to deploy my Flask API as it looks easiest to use and has minimal configuration required. You just upload your code and then AWS handles the rest. Works great with Python and other programming languages.

For the database, I would use AWS Relational Database Service (RDS). This would probably require converting from SQLite to PostgreSQL. RDS has good database hosting, automatic backups, scaling, and works great with PostgreSQL. It also has a free option for small projects such as this one.

And finally for running the scripts on a schedule, I think using AWS Lambda combined with Amazon EventBridge would be ideal. EventBridge lets me run on a schedule at specific intervals which is great for running as often as necessary. Then Lambda executes my scripts without the need to manage any servers. This allows a simpler set up and ability to scale up. However, with some more research I have found that using AWS Glue would be better for larger or more complex jobs. There are probably better alternatives for the API deployment service and database when working with longer scripts, larger databases, etc., but all of these should run great for this specific set of data, scripts, and API code.


