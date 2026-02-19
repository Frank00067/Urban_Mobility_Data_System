from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, text
import os

app = Flask(__name__)
CORS(app)

# setup connection to database
DATABASE_URI = os.getenv('POSTGRES_DATABASE_URI')
engine = create_engine(DATABASE_URI)



# API endpoint to get summary statistics for trips
""" Return hign-level stats for the dashboard cards"""
@app.route('/api/summary', methods=['GET'])
def get_summary():
    query = text("""
        SELECT
            COUNT(*) AS total_trips,
            ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
            ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
            ROUND(AVG(average_speed_mph)::numeric, 3) AS avg_speed
        FROM trips
        """)
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
    return jsonify({
        'total_trips': result[0],
        'avg_fare': float(result[1]) if result[1] is not None else 0.0,
        'avg_distance': float(result[2]),
        'avg_speed': float(result[3])
    })


# API endpoint to return the spatial boundaries of all zones for map visualization
@app.route('/api/zones', methods=['GET'])
def get_zones():
    query = text("select location_id, geometry from spatial_zones")

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    # formating as GeoJSON for FeatureCollection
    features = []
    for row in result:
        features.append({
            'type': 'Feature',
            'properties': {
                'location_id': row[0]
            },
            'geometry': row[1]
        })
    return jsonify({
        'type': 'FeatureCollection',
        'features': features
    })


# api endpoint to return the top 10 bsiest pickup zones with names and boroughs
@app.route('/api/top-locations', methods=['GET'])
def get_top_locations():
    query = text("""
            SELECT
                z.zone,
                z.borough,
                COUNT(*) AS trip_count
            FROM trips t
            JOIN zones z ON t.pu_location_id = z.location_id
            GROUP BY z.zone, z.borough
            ORDER BY trip_count DESC
            LIMIT 10;
        """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    # format the results
    top_ten = []
    for row in result:
        top_ten.append({
            'zone': row[0],
            'borough': row[1],
            'trip_count': row[2]
        })

    return jsonify(top_ten)


# Trips per day(jan 1 to jan 31)
@app.route('/api/trends', methods=['GET'])
def get_trends():
    query = text("""
        select
            date(pickup_datetime) as trip_date,
            count(*) as trip_count
        from trips
        group by trip_date
        order by trip_date;
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    trends = []
    for row in result:
        trends.append({
            "date": row[0].strftime('%Y-%m-%d'),
            "trip_count": row[1]
        })
    return jsonify(trends)



# derived Features API endpoint. This will return avg speed and tip percentage grouped by borough.
@app.route('/api/derived-features', methods=['GET'])
def get_derived_feature():
    query = text("""
        select
            z.borough,
            round(avg(t.average_speed_mph)::numeric, 2) as avg_speed,
            round(avg(t.tip_percentage)::numeric, 2) as avg_tip_pct,
            count(*) as trip_count
        from trips t
        join zones z on t.pu_location_id = z.location_id
        where z.borough != 'Unknown'
        group by z.borough
        order by avg_speed desc
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    stats = []
    for row in result:
        stats.append({
            'borough': row[0],
            'avg_speed_mph': float(row[1]),
            'avg_tip_percentage': float(row[2]),
            'trip_count': row[3]
        })
    return jsonify(stats)
        



if __name__ == '__main__':
    print("Starting Flask API server...")
    app.run(debug=True, port=5000)

