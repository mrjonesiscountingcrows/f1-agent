CREATE TABLE IF NOT EXISTS races (
    id          INTEGER PRIMARY KEY,
    year        INTEGER,
    round       INTEGER,
    gp_name     TEXT,
    country     TEXT,
    session     TEXT,
    date        DATE,
    UNIQUE(year, round, session)
);

CREATE TABLE IF NOT EXISTS results (
    id              INTEGER PRIMARY KEY,
    race_id         INTEGER REFERENCES races(id),
    position        INTEGER,
    driver_code     TEXT,
    driver_name     TEXT,
    team            TEXT,
    points          REAL,
    status          TEXT,
    fastest_lap     BOOLEAN
);

CREATE TABLE IF NOT EXISTS laps (
    id              INTEGER PRIMARY KEY,
    race_id         INTEGER REFERENCES races(id),
    driver_code     TEXT,
    lap_number      INTEGER,
    lap_time_ms     INTEGER,
    sector1_ms      INTEGER,
    sector2_ms      INTEGER,
    sector3_ms      INTEGER,
    compound        TEXT,
    tyre_life       INTEGER,
    is_personal_best BOOLEAN
);

CREATE TABLE IF NOT EXISTS driver_standings (
    id          INTEGER PRIMARY KEY,
    year        INTEGER,
    round       INTEGER,
    driver_code TEXT,
    driver_name TEXT,
    team        TEXT,
    points      REAL,
    position    INTEGER,
    wins        INTEGER
);

CREATE TABLE IF NOT EXISTS constructor_standings (
    id       INTEGER PRIMARY KEY,
    year     INTEGER,
    round    INTEGER,
    team     TEXT,
    points   REAL,
    position INTEGER,
    wins     INTEGER
);