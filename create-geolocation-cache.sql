CREATE TABLE geolocation_cache (
  id INTEGER,
  location VARCHAR(128) NOT NULL UNIQUE,
  provenance VARCHAR(32),
  query_time DATETIME,
  latitude FLOAT,
  longitude FLOAT,
  PRIMARY KEY (ID)
);
