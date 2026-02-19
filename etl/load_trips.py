import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np

# setup connection to database
DATABASE_URI = os.getenv('POSTGRES_DATABASE_URI')
engine = create_engine(DATABASE_URI)

def clear_trips_table():
    # clear existing data in trips table before loading new data
    print("Clearing existing data from trips table...")
    with engine.connect() as conn:
        conn.execute(text("truncate table trips cascade;"))
        conn.commit()
    print("Trips table cleared.")

def load_trips_data():
    print("Reading trip data from CSV...")
    file_path = 'data/yellow_tripdata_2019-01.csv'
    print(f"Starting ETL process for file: {file_path}")

    # read the CSV file in chunks to handle large files efficiently
    chunks = pd.read_csv(file_path, chunksize=100000)
    total_inserted = 0

    for i, df in enumerate(chunks):
        print(f"Processing chunk{i+1}...")

        # convert time string to actual python dataetime object

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        #select only Jan 2019 data
        mask = (df['tpep_pickup_datetime']>= '2019-01-01') & (df['tpep_pickup_datetime'] < '2019-02-01')
        df = df[mask].copy()

        # remove impossible values (negative fare, zero distance, etc)
        df = df[df['total_amount'] >= 0]
        df = df[df['trip_distance'] >= 0]
        df = df[df['passenger_count'] >= 0]

        """
        FEATURE ENGINEERING (DERIVE NEW FEATURES TO IMPROVE FRONTEND ANALYTICS)
        """

        # trip duration in seconds
        df['trip_duration_seconds'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds().astype(int)
        # remove trips with negative or zero duration
        df = df[df['trip_duration_seconds'] > 0]

        # average speed in miles per hour
        df['average_speed_mph'] = df['trip_distance'] / (df['trip_duration_seconds'] / 3600)
        df['average_speed_mph'] = df['average_speed_mph'].replace([np.inf, -np.inf], 0).clip(upper=99999999.99)

        # Tip percentage
        # handle cases where fare_amount is zero to avoid division by zero
        df['tip_percentage'] = np.where(
            df['fare_amount'] > 0,
            (df['tip_amount'] / df['fare_amount']) * 100,
            0
        )
        df['tip_percentage'] = df['tip_percentage'].clip(upper=999.99)

        # --- LOADING DATA INTO DATABASE ---
        print(f"Inserting chunk{i+1} into database...")
        
        # rename columns to match our postgres schema
        df = df.rename(columns={
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'RatecodeID': 'rate_code_id',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_location_id',   
        })

        # select only columns we need for our database(matching our postgres 'trips' table)

        columns_to_keep = [
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'passenger_count',
            'trip_distance',
            'rate_code_id',
            'store_and_fwd_flag',
            'pu_location_id',
            'do_location_id',
            'payment_type',
            'fare_amount',
            'extra',
            'mta_tax',
            'tip_amount',
            'tolls_amount',
            'improvement_surcharge',
            'total_amount',
            'trip_duration_seconds',
            'average_speed_mph',
            'tip_percentage'
        ]

        # push to Postgres using to_sql (using multi insert for better performance)
        final_df = df[columns_to_keep]
        final_df.to_sql('trips', engine, if_exists='append', index=False, method='multi')
        total_inserted += len(final_df)
        print(f"Finished chunk{i+1}. Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    try:
        clear_trips_table()
        load_trips_data()
        print("done! Trip data loaded and features calculated.")
    except Exception as e:
        print(f"Error during ETL process!")