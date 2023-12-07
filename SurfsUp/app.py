from flask import Flask, jsonify
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from datetime import datetime, timedelta


# Create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

# Create Flask app
app = Flask(__name__)

# Set up routes

@app.route("/")
def home():
    return (
        f"Welcome to the Climate App!<br/>"
        f"please make sure after start_date rout enter your desired date<br/>"
        f"example !! start_date/2015-01-01<br/>"
        f"and the same for start_date/end_date/2010-01-01/2015-01-01<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>" 
        f"/api/v1.0/start_date/end_date"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    last_date = session.query(func.max(Measurement.date)).scalar()
    one_year_ago = pd.to_datetime(last_date) - pd.DateOffset(days=365)
    
    # Convert Timestamp to string
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    precipitation_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago_str).all()

    precipitation_dict = {date: prcp for date, prcp in precipitation_data}

    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    station_list = session.query(Station.station).all()
    stations = [station[0] for station in station_list]

    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def tobs():
    most_active_station_id = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count().desc()).first()[0]

    last_date = session.query(func.max(Measurement.date)).filter(Measurement.station == most_active_station_id).scalar()
    one_year_ago = pd.to_datetime(last_date) - pd.DateOffset(days=365)
    
    # Convert Timestamp to string
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    temperature_data = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= one_year_ago_str).\
        filter(Measurement.station == most_active_station_id).all()

    temperature_list = [{"date": date, "tobs": tobs} for date, tobs in temperature_data]

    return jsonify(temperature_list)

@app.route("/api/v1.0/start_date/<start>")
def start_date(start):
    try:
        # Convert the start date parameter to a datetime object
        start_date = datetime.strptime(start, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use 'YYYY-MM-DD'."}), 400

    # Check if start date is within valid range
    min_date = session.query(func.min(Measurement.date)).scalar()

    if min_date is not None:  # Check if min_date is not None
        min_date = datetime.strptime(min_date, '%Y-%m-%d')
        max_date = session.query(func.max(Measurement.date)).scalar()
        max_date = datetime.strptime(max_date, '%Y-%m-%d')

        if start_date < min_date or start_date > max_date:
            return jsonify({"error": "Start date is outside the valid range. Please use a date within the range %s to %s" % (min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'))}), 400

        # Query for temperature data based on the start date
        results = session.query(func.min(Measurement.tobs).label('min_temp'),
                                func.avg(Measurement.tobs).label('avg_temp'),
                                func.max(Measurement.tobs).label('max_temp')).\
            filter(Measurement.date >= start_date).all()

        # Extract the results into a dictionary
        temperature_stats = {
            "min_temperature": results[0].min_temp,
            "avg_temperature": results[0].avg_temp,
            "max_temperature": results[0].max_temp
        }

        return jsonify(temperature_stats)
    else:
        return jsonify({"error": "No data available"}), 400

@app.route("/api/v1.0/start_date/end_date/<start>/<end>")
def start_end_date(start, end):
    try:
        # Convert the start and end date parameters to datetime objects
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use 'YYYY-MM-DD'."}), 400

    # Check if start date is within valid range
    min_date = session.query(func.min(Measurement.date)).scalar()

    if min_date is not None:  # Check if min_date is not None
        min_date = datetime.strptime(min_date, '%Y-%m-%d')
        max_date = session.query(func.max(Measurement.date)).scalar()
        max_date = datetime.strptime(max_date, '%Y-%m-%d')

        if start_date < min_date or start_date > max_date or end_date < min_date or end_date > max_date:
            return jsonify({"error": "Dates are outside the valid range. Please use dates within the range %s to %s" % (min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'))}), 400

        # Query for temperature data based on the date range
        results = session.query(func.min(Measurement.tobs).label('min_temp'),
                                func.avg(Measurement.tobs).label('avg_temp'),
                                func.max(Measurement.tobs).label('max_temp')).\
            filter(Measurement.date >= start_date).\
            filter(Measurement.date <= end_date).all()

        # Extract the results into a dictionary
        temperature_stats = {
            "min_temperature": results[0].min_temp,
            "avg_temperature": results[0].avg_temp,
            "max_temperature": results[0].max_temp
        }

        return jsonify(temperature_stats)
    else:
        return jsonify({"error": "No data available"}), 400

if __name__ == "__main__":
    app.run(debug=True)