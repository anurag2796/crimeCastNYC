-- Enable PostGIS if needed (optional)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Create main fact table with Partitioning
CREATE TABLE IF NOT EXISTS calls_for_service (
    cad_evnt_id VARCHAR(20) NOT NULL,
    created_date TIMESTAMP,
    incident_date TIMESTAMP,
    incident_time TIME,
    ny_cli VARCHAR(50),
    arrival_time TIMESTAMP,
    closing_time TIMESTAMP,
    vol_id VARCHAR(50),
    precinct_id INT,
    sector_id VARCHAR(10),
    borough TEXT,
    patrol_boro TEXT,
    complaint_type TEXT,
    descriptor TEXT,
    location_type_code VARCHAR(50),
    city TEXT,
    latitude FLOAT,
    longitude FLOAT,
    
    -- Partition Key
    incident_year INT GENERATED ALWAYS AS (EXTRACT(YEAR FROM incident_date)) STORED,
    
    PRIMARY KEY (cad_evnt_id, incident_year)
) PARTITION BY RANGE (incident_year);
-- Note: 'incident_date' is usually reliable, but we use a generated column for partitioning efficiency if we want simplified range logic.
-- Actually, strict partitioning by TIMESTAMP range is standard. Let's use that.

DROP TABLE IF EXISTS calls_for_service cascade;

CREATE TABLE calls_for_service (
    cad_evnt_id VARCHAR(20) NOT NULL,
    created_date TIMESTAMP,
    incident_date TIMESTAMP NOT NULL, -- Required for partitioning
    incident_time TIME,
    ny_cli VARCHAR(50),
    arrival_time TIMESTAMP,
    closing_time TIMESTAMP,
    vol_id VARCHAR(50),
    precinct_id INT,
    sector_id VARCHAR(10),
    borough TEXT,
    patrol_boro TEXT,
    complaint_type TEXT,
    descriptor TEXT,
    location_type_code VARCHAR(50),
    city TEXT,
    latitude FLOAT,
    longitude FLOAT
) PARTITION BY RANGE (incident_date);

-- Create partitions for 20 years (2004-2025)
-- We can script this or just generate them statically.
-- Generating standard years.
CREATE TABLE calls_2004 PARTITION OF calls_for_service FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');
CREATE TABLE calls_2005 PARTITION OF calls_for_service FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');
CREATE TABLE calls_2006 PARTITION OF calls_for_service FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE calls_2007 PARTITION OF calls_for_service FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE calls_2008 PARTITION OF calls_for_service FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE calls_2009 PARTITION OF calls_for_service FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE calls_2010 PARTITION OF calls_for_service FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE calls_2011 PARTITION OF calls_for_service FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE calls_2012 PARTITION OF calls_for_service FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE calls_2013 PARTITION OF calls_for_service FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE calls_2014 PARTITION OF calls_for_service FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE calls_2015 PARTITION OF calls_for_service FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE calls_2016 PARTITION OF calls_for_service FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE calls_2017 PARTITION OF calls_for_service FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE calls_2018 PARTITION OF calls_for_service FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE calls_2019 PARTITION OF calls_for_service FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE calls_2020 PARTITION OF calls_for_service FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE calls_2021 PARTITION OF calls_for_service FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE calls_2022 PARTITION OF calls_for_service FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE calls_2023 PARTITION OF calls_for_service FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE calls_2024 PARTITION OF calls_for_service FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
-- Catch-all for data outside range
CREATE TABLE calls_default PARTITION OF calls_for_service DEFAULT;

-- Indices on the main table (Propagates to partitions)
CREATE INDEX idx_incident_date ON calls_for_service (incident_date);
CREATE INDEX idx_complaint_type ON calls_for_service (complaint_type);
CREATE INDEX idx_borough ON calls_for_service (borough);
