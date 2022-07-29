-- Q5. Flight Hopping

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel;
DROP TABLE IF EXISTS q5 CASCADE;

CREATE TABLE q5 (
	destination CHAR(3),
	num_flights INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS start_flight CASCADE;
DROP VIEW IF EXISTS day CASCADE;
DROP VIEW IF EXISTS n CASCADE;

CREATE VIEW day AS
SELECT day::date as day FROM q5_parameters;
-- can get the given date using: (SELECT day from day)

CREATE VIEW n AS
SELECT n FROM q5_parameters;
-- can get the given number of flights using: (SELECT n from n)

-- p.s. according to air_travel.ddl, table q5_parameters has only 1 row

-- 1.
-- Info of
-- Flights that depart from YYZ on this day
CREATE VIEW start_flight AS
SELECT *
FROM Flight
WHERE outbound = 'YYZ' AND DATE(s_dep) IN (
    SELECT day
    FROM day
);

-- 2.
-- Recursively get
-- Destination, s_arv, num_flights of
-- Hopping flights
WITH RECURSIVE answer_q5 AS (
    (SELECT inbound AS destination,
            s_arv,
            1 AS num_flights
     FROM start_flight)
    UNION ALL
    (SELECT Flight.inbound AS destination,
            Flight.s_arv,
            num_flights + 1
     FROM answer_q5 JOIN Flight ON Flight.outbound = answer_q5.destination
     WHERE (Flight.s_dep - answer_q5.s_arv < INTERVAL '24 hours') AND (
     num_flights < ALL (
        SELECT n
        FROM n ))
    )
)

-- HINT: You can answer the question by writing one recursive query below, without any more views.
-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q5 (
SELECT destination, num_flights
FROM answer_q5
);
















