CREATE TABLE calls (
  digest VARCHAR(40) NOT NULL,
  time_received DATETIME NOT NULL,
  agency VARCHAR(16) NOT NULL,
  dispatch_area VARCHAR(32) NOT NULL,
  unit VARCHAR(16) NOT NULL,
  call_type VARCHAR(128) NOT NULL,
  location VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (digest)
);

