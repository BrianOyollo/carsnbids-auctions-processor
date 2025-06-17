/* 
================================================================
 SQL Script for Creating Data Warehouse Schema
================================================================
This SQL script is designed to create the schema for the data warehouse used in the vehicle auction project. 
It contains statements to create the following:

1. Staging Table (staging_auction_data):
   - A temporary storage table where raw, cleaned, and transformed auction and vehicle data will be loaded initially.
   - It includes all fields needed for the dimensional and fact tables.

2. Dimension Tables:
   - vehicle_dim: Contains information about the vehicle attributes such as VIN, make, model, and manufacture year.
   - auction_dim: Contains auction-related attributes including auction status, title, and date.

3. Fact Table (auction_fact):
   - Contains aggregated data such as bid counts, highest bid values, and other metrics for each auction entry.
*/


/*
====================================================================
                    STAGING TABLE
====================================================================
*/

CREATE TABLE staging (
    auction_date BIGINT,
    auction_id TEXT,
    vin TEXT,
    seller_type TEXT,
    reserve_status TEXT,
    reserve_met BOOLEAN,
    auction_status TEXT,
    auction_title TEXT,
    auction_subtitle TEXT,
    make TEXT,
    model TEXT,
    exterior_color TEXT,
    interior_color TEXT,
    body_style TEXT,
    mileage INT,
    engine TEXT,
    drivetrain TEXT,
    transmission TEXT,
    transmission_type TEXT,
    gears INT,
    title_status_cleaned TEXT,
    title_state TEXT,
    city TEXT,
    state TEXT,
    bid_count INT,
    view_count INT,
    watcher_count INT,
    highest_bid_value INT,
    max_bid INT,
    min_bid INT,
    mean_bid INT,
    median_bid INT,
    bid_range INT,
    bids INTEGER[], 
    highlight_count INT,
    equipment_count INT,
    mod_count INT,
    flaw_count INT,
    service_count INT,
    included_items_count INT,
    video_count INT,
    manufacture_year INT,
    location TEXT,
    auction_url TEXT,
    seller TEXT
);

/*
====================================================================
                    DIM TABLES
====================================================================
*/

-- Normalized supporting tables
CREATE TABLE body_style_dim (
    id SERIAL PRIMARY KEY,
    body_style TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE drivetrain_dim (
    id SERIAL PRIMARY KEY,
    drivetrain TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE state_dim (
    id SERIAL PRIMARY KEY,
    state TEXT,
    state_abbr TEXT UNIQUE,
    country_code TEXT DEFAULT 'US',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transmission_dim (
    id SERIAL PRIMARY KEY,
    transmission TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE city_dim (
    id SERIAL PRIMARY KEY,
    city_name TEXT,
    state_id INT REFERENCES state_dim(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_name, state_id)
);

CREATE TABLE auction_status_dim (
    id SERIAL PRIMARY KEY,
    status TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE reserve_status_dim (
    id SERIAL PRIMARY KEY,
    status TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE seller_type_dim (
    id SERIAL PRIMARY KEY,
    seller_type TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE vehicle_make_dim (
    id SERIAL PRIMARY KEY,
    make TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE vehicle_model_dim (
    id SERIAL PRIMARY KEY,
    model TEXT,
    make_id INT REFERENCES vehicle_make_dim(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(model, make_id)

);


-- Vehicle Dimension
CREATE TABLE vehicle_dim (
    vehicle_id SERIAL PRIMARY KEY,
    vin TEXT,
    auction_id TEXT,
    make_id INT REFERENCES vehicle_make_dim(id),
    model_id INT REFERENCES vehicle_model_dim(id),
    body_style_id INT REFERENCES body_style_dim(id),
    manufacture_year INT,
    mileage BIGINT,
    engine TEXT,
    transmission_id INT REFERENCES transmission_dim(id),
    gear_count INT,
    drivetrain_id INT REFERENCES drivetrain_dim(id),
    exterior_color TEXT,
    interior_color TEXT,
    title_status TEXT,
    title_state TEXT,
    equipment_count INT,
    mod_count INT,
    flaw_count INT,
    service_count INT,
    included_items_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vin, auction_id)
);


/*
====================================================================
                    FACT TABLES
====================================================================
*/

CREATE TABLE auction_fact (
	auction_id TEXT PRIMARY KEY,
    auction_time TIMESTAMPTZ,
    vehicle_id INT REFERENCES vehicle_dim(vehicle_id),
    auction_status INT REFERENCES auction_status_dim(id),
    reserve_status INT REFERENCES reserve_status_dim(id),
    auction_state INT REFERENCES state_dim(id),
    auction_city INT REFERENCES city_dim(id),
    seller_type INT REFERENCES seller_type_dim(id),
	view_count INT,
    watcher_count INT,
    bid_count INT,
    max_bid NUMERIC(12,2),
	min_bid NUMERIC(12,2),
    mean_bid NUMERIC(12,2),
    median_bid NUMERIC(12,2),
    bid_range NUMERIC(12,2),
    bids INTEGER[],
    highlight_count INT,
    video_count INT,
    auction_title TEXT,
    auction_subtitle TEXT,
    auction_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

