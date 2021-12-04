CREATE TABLE labels (
    label_id SERIAL PRIMARY KEY,
    name VARCHAR(20) UNIQUE NOT NULL
);

CREATE TABLE samples (
    sample_id SERIAL PRIMARY KEY,
    label_id INTEGER NOT NULL,
    measurement_time TIMESTAMP NOT NULL,
    value REAL NOT NULL,
    FOREIGN KEY (label_id) REFERENCES labels (label_id)
);