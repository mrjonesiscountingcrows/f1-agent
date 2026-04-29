CREATE SEQUENCE IF NOT EXISTS races_id_seq;
CREATE SEQUENCE IF NOT EXISTS results_id_seq;
CREATE SEQUENCE IF NOT EXISTS laps_id_seq;
CREATE SEQUENCE IF NOT EXISTS driver_standings_id_seq;
CREATE SEQUENCE IF NOT EXISTS constructor_standings_id_seq;

CREATE TABLE IF NOT EXISTS races (
    id          BIGINT PRIMARY KEY DEFAULT nextval('races_id_seq'),
    year        INTEGER,
    round       INTEGER,
    gp_name     TEXT,
    country     TEXT,
    session     TEXT,
    date        DATE,
    UNIQUE(year, round, session)
);

CREATE TABLE IF NOT EXISTS results (
    id              BIGINT PRIMARY KEY DEFAULT nextval('results_id_seq'),
    race_id         BIGINT REFERENCES races(id),
    position        INTEGER,
    driver_code     TEXT,
    driver_name     TEXT,
    team            TEXT,
    points          REAL,
    status          TEXT,
    fastest_lap     BOOLEAN
);

CREATE TABLE IF NOT EXISTS laps (
    id              BIGINT PRIMARY KEY DEFAULT nextval('laps_id_seq'),
    race_id         BIGINT REFERENCES races(id),
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
    id          BIGINT PRIMARY KEY DEFAULT nextval('driver_standings_id_seq'),
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
    id       BIGINT PRIMARY KEY DEFAULT nextval('constructor_standings_id_seq'),
    year     INTEGER,
    round    INTEGER,
    team     TEXT,
    points   REAL,
    position INTEGER,
    wins     INTEGER
);

CREATE SEQUENCE IF NOT EXISTS qualifying_results_id_seq;

CREATE TABLE IF NOT EXISTS qualifying_results (
    id              BIGINT PRIMARY KEY DEFAULT nextval('qualifying_results_id_seq'),
    race_id         BIGINT REFERENCES races(id),
    position        INTEGER,
    driver_code     TEXT,
    driver_name     TEXT,
    team            TEXT,
    q1_ms           INTEGER,
    q2_ms           INTEGER,
    q3_ms           INTEGER,
    best_time_ms    INTEGER
);

CREATE SEQUENCE IF NOT EXISTS sprint_results_id_seq;
CREATE SEQUENCE IF NOT EXISTS sprint_qualifying_results_id_seq;

CREATE TABLE IF NOT EXISTS sprint_results (
    id              BIGINT PRIMARY KEY DEFAULT nextval('sprint_results_id_seq'),
    race_id         BIGINT REFERENCES races(id),
    position        INTEGER,
    driver_code     TEXT,
    driver_name     TEXT,
    team            TEXT,
    points          REAL,
    status          TEXT
);

CREATE TABLE IF NOT EXISTS sprint_qualifying_results (
    id              BIGINT PRIMARY KEY DEFAULT nextval('sprint_qualifying_results_id_seq'),
    race_id         BIGINT REFERENCES races(id),
    position        INTEGER,
    driver_code     TEXT,
    driver_name     TEXT,
    team            TEXT,
    sq1_ms          INTEGER,
    sq2_ms          INTEGER,
    sq3_ms          INTEGER,
    best_time_ms    INTEGER
);