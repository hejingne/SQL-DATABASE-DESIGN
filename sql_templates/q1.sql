-- Q1. Airlines

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel;
DROP TABLE IF EXISTS q1 CASCADE;

CREATE TABLE q1 (
    pass_id INT,
    name VARCHAR(100),
    airlines INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS psg_flied CASCADE;
DROP VIEW IF EXISTS psg_never_flied CASCADE;
DROP VIEW IF EXISTS answer_q1 CASCADE;


-- Define views for your intermediate steps here:

-- 1.
-- Id, name, (num of airlines) of
-- Passengers who have taken at least 1 flights
CREATE VIEW psg_flied AS
SELECT Passenger.id AS pass_id,
       firstname||' '||surname as name,
       count(DISTINCT airline) as airlines
FROM Booking JOIN Flight ON flight_id = Flight.id
     JOIN Passenger ON pass_id = Passenger.id
GROUP BY Passenger.id;

-- 2.
-- Id, name, (num of airlines) of
-- Passengers who have never taken a flight
CREATE VIEW psg_never_flied AS
SELECT id AS pass_id,
       firstname||' '||surname as name,
       0 as airlines
FROM Passenger
WHERE NOT EXISTS (
    SELECT *
    FROM Booking
    WHERE pass_id = Passenger.id
);

-- 3.
-- Combine both views to get desired table
CREATE VIEW answer_q1 AS
(SELECT * FROM psg_flied)
UNION
(SELECT * FROM psg_never_flied);

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q1 (
SELECT pass_id, name, airlines
FROM answer_q1
ORDER BY pass_id    -- sort results for better readability
);
