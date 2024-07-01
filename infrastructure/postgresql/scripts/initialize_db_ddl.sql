/*Reset DB Before Recreating it*/
DROP TABLE IF EXISTS property_listing;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS regions;
DROP TABLE IF EXISTS states;
DROP TABLE IF EXISTS property_types;
DROP TABLE IF EXISTS laundry_options;
DROP TABLE IF exists parking_options;


/*Create DB Structure*/
CREATE TABLE IF NOT EXISTS states(
    state_id             SERIAL       NOT NULL PRIMARY KEY,
    description          VARCHAR(50)  NOT NULL,
    acronym              VARCHAR(2)   NOT NULL,
    active               BOOLEAN      NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3) NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS regions(
    region_id            SERIAL        NOT NULL PRIMARY KEY,
    description          VARCHAR(100)  NOT NULL,
    region_url           VARCHAR(1000),
    active               BOOLEAN       NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS locations(
    location_id          SERIAL      NOT NULL PRIMARY KEY,
    region_id            INTEGER,
    state_id             INTEGER,
    active               BOOLEAN      NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3) NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (state_id) REFERENCES states(state_id)
);

CREATE TABLE IF NOT EXISTS property_types(
    property_type_id SERIAL           NOT NULL PRIMARY KEY,
    description VARCHAR(100)          NOT NULL,
    active               BOOLEAN      NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3) NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS laundry_options(
    laundry_option_id    SERIAL       NOT NULL PRIMARY KEY,
    description          VARCHAR(100) NOT NULL,
    active               BOOLEAN      NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3) NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS parking_options(
    parking_option_id    SERIAL       NOT NULL PRIMARY KEY,
    description          VARCHAR(100) NOT NULL,
    active               BOOLEAN      NOT NULL DEFAULT TRUE,
    utc_datetime_created TIMESTAMP(3)  NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted TIMESTAMP(3),
    utc_datetime_updated TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS property_listing (
    property_listing_id         BIGINT        NOT NULL PRIMARY KEY,
    property_listing_url        VARCHAR(500),
    property_image_url          VARCHAR(500),
    property_description        TEXT,
    property_location_id        INTEGER,
    property_location_longitude DECIMAL,
    property_location_latitude  DECIMAL,
    property_type_id            INTEGER       NOT NULL,
    laundry_option_id           INTEGER,
    parking_option_id           INTEGER,
    property_square_feet        DECIMAL       NOT NULL,
    property_price              DECIMAL       NOT NULL,
    bedrooms                    SMALLINT      NOT NULL,
    bathrooms                   SMALLINT      NOT NULL,
    cats_allowed                BOOLEAN       NOT NULL DEFAULT FALSE,
    dogs_allowed                BOOLEAN       NOT NULL DEFAULT FALSE,
    smoking_allowed             BOOLEAN       NOT NULL DEFAULT FALSE,
    wheelchair_access           BOOLEAN       NOT NULL DEFAULT FALSE,
    comes_furnished             BOOLEAN       NOT NULL DEFAULT FALSE,
    electric_vehicle_charge     BOOLEAN       NOT NULL DEFAULT FALSE,
    active                      BOOLEAN       NOT NULL DEFAULT TRUE,
    utc_datetime_created        TIMESTAMP(3)  NOT null DEFAULT CURRENT_TIMESTAMP(3),
    utc_datetime_deleted        TIMESTAMP(3),
    utc_datetime_updated        TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    FOREIGN KEY(property_location_id) REFERENCES locations(location_id),
    FOREIGN KEY(property_type_id) REFERENCES property_types(property_type_id),
    FOREIGN KEY(laundry_option_id) REFERENCES laundry_options(laundry_option_id),
    FOREIGN KEY(parking_option_id) REFERENCES parking_options(parking_option_id)
);