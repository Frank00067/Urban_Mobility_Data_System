import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import json
import os

# setup connection to database
DATABASE_URI = os.getenv('POSTGRES_DATABASE_URI')
engine = create_engine(DATABASE_URI)

def clear_tables():
    # clear existing data in tables before loading new data
    print("Clearing existing data from zones and spatial_zones tables...")
    with engine.connect() as conn:
        conn.execute(text("truncate table spatial_zones cascade;"))
        conn.execute(text("truncate table zones cascade;"))
        conn.commit()
    print("Tables cleared.")


# load zones data
def load_zones_data():
    print("Reading taxi_zone_lookup.csv...")
    zones_df = pd.read_csv('data/taxi_zone_lookup.csv')

    # rename columns to match our database schema
    zones_df.columns = ['location_id', 'borough', 'zone', 'service_zone']

    # drop duplicates if any
    zones_df = zones_df.drop_duplicates(subset=['location_id'])

    print("Inserting zones data into database...")
    zones_df.to_sql('zones', engine, if_exists='append', index=False)
    print(f"Inserted {len(zones_df)} records into zones table.")


def load_spatial_data():
    print("Reading spatial data from shepefile...")
    gdf = gpd.read_file('data/taxi_zones.zip')

    # merge multiple rows with same LocationID by dissolving geometries
    print("Merging mult-part zones with same LocationID...")
    gdf = gdf.dissolve(by='LocationID').reset_index()

    # convert coordinates to standard GPS format (WGS84)
    gdf = gdf.to_crs(epsg=4326)

    # extract locationID and the geometry(the shape)
    spatial_data = []
    for _, row in gdf.iterrows():
        spatial_data.append({
            'location_id': int(row['LocationID']),
            'geometry': json.dumps(row['geometry'].__geo_interface__)
        })

    # convert list to dataframe and push to 'spatial_zones' table
    spatial_df = pd.DataFrame(spatial_data)
    print("Inserting spatial data into database...")
    spatial_df.to_sql('spatial_zones', engine, if_exists='append', index=False)
    print(f"Successfully loaded {len(spatial_df)} spatial boundaries.")


if __name__ == "__main__":
    try:
        clear_tables()
        load_zones_data()
        load_spatial_data()
        print("done! zones and spatial data loaded into database.")
    except Exception as e:
        print(f"Error loading data: {e}")
