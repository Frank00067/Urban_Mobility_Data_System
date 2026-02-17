-- drop database if exists nyc_taxi_data;
-- CREATE DATABASE nyc_taxi_data;
-- \c nyc_taxi_data;
-- /*

drop table if exists trips;
drop table if exists spatial_zones;
drop table if exists zones;

-- */

-- Create Dimension table that store mapping from ID to neighborhood (zones)

create table zones (
    location_id integer primary key,
    borough varchar(255),
    zone varchar(255),
    service_zone varchar(255)
);

-- create the spatial metadata table for storing geoJSON data for map visualization
create table spatial_zones (
    location_id integer primary key references zones(location_id), 
    geometry JSONB not null
);

-- Create fact table that store the trip data(pu_location_id, do_location_id, trip_distance, fare_amount, tip_amount, total_amount, payment_type, pickup_datetime, dropoff_datetime
create table trips (
    trip_id serial primary key,
    vendor_id integer,
    pickup_datetime timestamp not null,
    dropoff_datetime timestamp not null,
    passenger_count smallint,
    trip_distance numeric(10,2),
    rate_code integer,
    store_and_fwd_flag char(1),
    pu_location_id integer references zones(location_id),
    do_location_id integer references zones(location_id),
    payment_type integer,
    fare_amount numeric(10,2),
    extra numeric(10,2),
    mta_tax numeric(10,2),
    tip_amount numeric(10,2),
    tolls_amount numeric(10,2),
    improvement_surcharge numeric(10,2),
    total_amount numeric(10,2),
    congestion_surcharge numeric(10,2),

    -- these columns are for derived features, which will be calculated by our python script
    trip_duration_seconds integer,
    average_speed numeric(10,2),
    tip_percentage numeric(5,2)
);

-- performance indexing( for faster query on trip data in frontend)
create index idx_pickup_time on trips (pickup_datetime);
create index idx_pickup_location on trips(pu_location_id);
create index idx_dropoff_location on trips(do_location_id);
create index idx_total_amount on trips(total_amount);