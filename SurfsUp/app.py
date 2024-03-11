# Import the dependencies.
from flask import Flask, jsonify, g
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import and_, func, create_engine
from sqlalchemy.orm import Session
from datetime import datetime

#################################################
# Database Setup
#################################################

# create an engine to connect to database
engine = create_engine("sqlite:///hawaii.sqlite")
# declare a base 
Base = automap_base()
# use base class to reflect tables 
Base.prepare(autoload_with=engine)
# assign 'measurement' class to variable 
measurement = Base.classes.measurement
# assign 'station' class to variable 
station = Base.classes.station
# create session 
session = Session(engine)

# create flask app
app = Flask(__name__)

# define homepage route
@app.route('/')
def homepage():
    routes = [str(rule) for rule in app.url_map.iter_rules()]
    return jsonify({'message': 'Welcome to the homepage - please select a route:', 'routes': routes})

# define route list available routes
@app.route('/list_routes')
def list_routes():
    routes = [str(rule) for rule in app.url_map.iter_rules()]
    return jsonify({'routes': routes})

#################################################
# Flask Routes
#################################################

# precipitation
@app.route("/api/v1.0/precipitation")
def precipitation():

    # assign dates for 12mo window from jupyter notebook analysis
    twelve_months_date = '2016-08-23'  
    most_recent = '2017-08-23'  

    # make sure both are dates 
    twelve_months_before = datetime.strptime(twelve_months_date, "%Y-%m-%d")
    most_recent_date = datetime.strptime(most_recent, "%Y-%m-%d")

    # query the database for date and prcp within the specified date range
    data = session.query(measurement.date, measurement.prcp).filter(
        and_(measurement.date >= twelve_months_before, measurement.date <= most_recent_date)
    ).all()

    # convert the query results to a list of dictionaries
    result = [{'date': row[0], 'prcp': row[1]} for row in data]

    # return the JSON representation of the data
    return jsonify(result)

# stations
@app.route("/api/v1.0/stations")
def stations():
    # query unique station names from database
    unique_station_names = session.query(station.station).distinct().all()

    # extract station names from result
    station_names = [result.station for result in unique_station_names]

    # jsonify results
    return jsonify(station_names)

# tobs (temperature)
@app.route("/api/v1.0/tobs")
def tobs():
    # grab most active station
    stmt = session.query(measurement.station, func.count().label('total_count')) \
            .group_by(measurement.station) \
            .order_by(func.count().desc())
    
    # top result from last query
    top_result = stmt.first()

    # get the values - assign to variable 
    top_column_value = top_result[0]

    # assign dates for 12mo window from jupyter notebook analysis
    twelve_months_date = '2016-08-23'  
    most_recent = '2017-08-23'  

    # make sure both are dates 
    twelve_months_before = datetime.strptime(twelve_months_date, "%Y-%m-%d")
    most_recent_date = datetime.strptime(most_recent, "%Y-%m-%d")

    # query the tobs data for the top station for the last 12 months
    results = session.query(measurement.date, measurement.tobs).filter(measurement.station == top_column_value,
                                              measurement.date.between(twelve_months_before, most_recent_date)).all()

    # extract the temperature values and dates - assign to variables
    temperature_dates = [{"date": result.date, "temperature": result.tobs} for result in results]

    # jsonify results
    return jsonify(temperature_dates)

# start - dynamic route 1
@app.route("/api/v1.0/<start>")
def temp_range(start):

    # format date properly
    start_date_clean = datetime.strptime(start, "%Y-%m-%d")
    
    # assign date column to variable
    filter_column = measurement.date
    
    # assign temperature column to variable
    count_column = measurement.tobs
    
    # build query to find lowest/highest/average temps after start date
    stmt_temperature = session.query(func.min(count_column), func.max(count_column), func.avg(count_column)) \
                                .filter(filter_column >= start_date_clean)
    
    # execute query and get results
    temp_results = stmt_temperature.first()
    
    # extract values from query result
    min_temp, max_temp, avg_temp = temp_results
    
    # create dictionary with extracted values
    result_dict = {
        'start_date': start_date_clean.strftime("%Y-%m-%d"),
        'min_temperature': float(min_temp),
        'max_temperature': float(max_temp),
        'avg_temperature': float(avg_temp)
    }
    
    # jsonify results
    return jsonify(result_dict)

# start/end dynamic route 2
@app.route("/api/v1.0/<start>/<end>")
def temp_full_range(start, end):

    # format dates properly
    start_date_clean = datetime.strptime(start, "%Y-%m-%d")
    end_date_clean = datetime.strptime(end, "%Y-%m-%d")

    # assign date column to variable
    filter_column = measurement.date

    # assign temperature column to variable
    count_column = measurement.tobs

    # build query to find lowest/highest/average temps after/on start date and before/on end date
    stmt_temperature = session.query(func.min(count_column), func.max(count_column), func.avg(count_column)) \
        .filter(and_(filter_column >= start_date_clean, filter_column <= end_date_clean))

    # execute query and get results
    temp_results = stmt_temperature.first()
    
    # extract values from query result
    min_temp, max_temp, avg_temp = temp_results
    
    # create dictionary with extracted values
    full_temp_dict = {
        'start_date': start_date_clean.strftime("%Y-%m-%d"),
        'end_date': end_date_clean.strftime("%Y-%m-%d"),
        'min_temperature': float(min_temp),
        'max_temperature': float(max_temp),
        'avg_temperature': float(avg_temp)
    }

    # jsonify
    return jsonify(full_temp_dict)

if __name__ == '__main__':
    app.run(debug=True)