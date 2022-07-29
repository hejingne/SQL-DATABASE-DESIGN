-- Q4. Plane Capacity Histogram

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4 (
	airline CHAR(2),
	tail_number CHAR(5),
	very_low INT,
	low INT,
	fair INT,
	normal INT,
	high INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS plane_capacity CASCADE;
DROP VIEW IF EXISTS departed_flight_seats CASCADE;
DROP VIEW IF EXISTS very_low_cap_plane CASCADE;
DROP VIEW IF EXISTS very_low_all CASCADE;
DROP VIEW IF EXISTS low_cap_plane CASCADE;
DROP VIEW IF EXISTS low_all CASCADE;
DROP VIEW IF EXISTS fair_cap_plane CASCADE;
DROP VIEW IF EXISTS fair_all CASCADE;
DROP VIEW IF EXISTS normal_cap_plane CASCADE;
DROP VIEW IF EXISTS normal_all CASCADE;
DROP VIEW IF EXISTS high_cap_plane CASCADE;
DROP VIEW IF EXISTS high_all CASCADE;
DROP VIEW IF EXISTS answer_q4 CASCADE;


-- Define views for your intermediate steps here:

-- 1.
-- Airline, tail_number,
-- total capacity (the sum of capacities of
-- economy & business & first classes) of
-- Planes
CREATE VIEW plane_capacity AS
SELECT airline,
       tail_number,
       (capacity_economy + capacity_business + capacity_first) AS capacity
FROM Plane;

-- 2.
-- Id, airline, tail_number, total number of booked seats of
-- Flights that have actually departed
CREATE VIEW departed_flight_seats AS
SELECT Flight.id,
       airline,
       plane AS tail_number,
       count(seat_class) AS booked_capacity
FROM Flight JOIN Booking ON Flight.id = Booking.flight_id
GROUP BY Flight.id, airline, plane;

-- 3. a.
-- Airline, tail_number,
-- total number of flights that had very low (>= 0% and < 20%) capacity of
-- Planes
CREATE VIEW very_low_cap_plane AS
SELECT plane_capacity.airline AS airline,
       plane_capacity.tail_number AS tail_number,
       count(id) AS very_low
FROM plane_capacity LEFT JOIN departed_flight_seats ON (
    plane_capacity.airline =  departed_flight_seats.airline AND
    plane_capacity.tail_number = departed_flight_seats.tail_number)
WHERE (booked_capacity IS NULL) OR
      (booked_capacity >= 0 AND booked_capacity < 0.2 * capacity)
GROUP BY plane_capacity.airline, plane_capacity.tail_number;

-- 3.b.
-- Combine very_low_cap_plane with 
-- Planes whose booked_capacity ISN'T null NOR very low
CREATE VIEW very_low_all AS
(SELECT airline, tail_number, 0 AS very_low
 FROM plane_capacity
 WHERE (airline, tail_number) NOT IN (
    SELECT airline, tail_number
    FROM very_low_cap_plane))
UNION
(SELECT * FROM very_low_cap_plane);

-- 4.a.
-- Airline, tail_number,
-- total number of flights that had low (>= 20% and < 40%) capacity of
-- Planes
CREATE VIEW low_cap_plane AS
SELECT plane_capacity.airline AS airline,
       plane_capacity.tail_number AS tail_number,
       count(id) AS low
FROM plane_capacity LEFT JOIN departed_flight_seats ON (
    plane_capacity.airline =  departed_flight_seats.airline AND
    plane_capacity.tail_number = departed_flight_seats.tail_number)
WHERE (booked_capacity IS NULL) OR
      (booked_capacity >= 0.2 * capacity AND booked_capacity < 0.4 * capacity)
GROUP BY plane_capacity.airline, plane_capacity.tail_number;

-- 4.b.
-- Combine low_cap_plane with
-- Planes whose booked_capacity ISN'T null NOR low
CREATE VIEW low_all AS
(SELECT airline, tail_number, 0 AS low
 FROM plane_capacity
 WHERE (airline, tail_number) NOT IN (
    SELECT airline, tail_number
    FROM low_cap_plane))
UNION
(SELECT * FROM low_cap_plane);

-- 5.a.
-- Airline, tail_number,
-- total number of flights that had fair (>= 40% and < 60%) capacity of
-- Planes
CREATE VIEW fair_cap_plane AS
SELECT plane_capacity.airline AS airline,
       plane_capacity.tail_number AS tail_number,
       count(id) AS fair
FROM plane_capacity LEFT JOIN departed_flight_seats ON (
    plane_capacity.airline =  departed_flight_seats.airline AND
    plane_capacity.tail_number = departed_flight_seats.tail_number)
WHERE (booked_capacity IS NULL) OR
      (booked_capacity >= 0.4 * capacity AND booked_capacity < 0.6 * capacity)
GROUP BY plane_capacity.airline, plane_capacity.tail_number;

-- 5.b.
-- Combine fair_cap_plane with
-- Planes whose booked_capacity ISN'T null NOR fair
CREATE VIEW fair_all AS
(SELECT airline, tail_number, 0 AS fair
 FROM plane_capacity
 WHERE (airline, tail_number) NOT IN (
    SELECT airline, tail_number
    FROM fair_cap_plane))
UNION
(SELECT * FROM fair_cap_plane);

-- 6.a.
-- Airline, tail_number,
-- total number of flights that had normal (>= 60% and < 80%) capacity of
-- Planes
CREATE VIEW normal_cap_plane AS
SELECT plane_capacity.airline AS airline,
       plane_capacity.tail_number AS tail_number,
       count(id) AS normal
FROM plane_capacity LEFT JOIN departed_flight_seats ON (
    plane_capacity.airline =  departed_flight_seats.airline AND
    plane_capacity.tail_number = departed_flight_seats.tail_number)
WHERE (booked_capacity IS NULL) OR
      (booked_capacity >= 0.6 * capacity AND booked_capacity < 0.8 * capacity)
GROUP BY plane_capacity.airline, plane_capacity.tail_number;

-- 6.b.
-- Combine normal_cap_plane with
-- Planes whose booked_capacity ISN'T null NOR normal
CREATE VIEW normal_all AS
(SELECT airline, tail_number, 0 AS normal
 FROM plane_capacity
 WHERE (airline, tail_number) NOT IN (
    SELECT airline, tail_number
    FROM normal_cap_plane))
UNION
(SELECT * FROM normal_cap_plane);

-- 7.a.
-- Airline, tail_number,
-- total number of flights that had high (>= 80%) capacity of
-- Planes
CREATE VIEW high_cap_plane AS
SELECT plane_capacity.airline AS airline,
       plane_capacity.tail_number AS tail_number,
       count(id) AS high
FROM plane_capacity LEFT JOIN departed_flight_seats ON (
    plane_capacity.airline =  departed_flight_seats.airline AND
    plane_capacity.tail_number = departed_flight_seats.tail_number)
WHERE (booked_capacity IS NULL) OR
      (booked_capacity >= 0.8 * capacity)
GROUP BY plane_capacity.airline, plane_capacity.tail_number;

-- 7.b.
-- Combine high_cap_plane with
-- Planes whose booked_capacity ISN'T null NOR high
CREATE VIEW high_all AS
(SELECT airline, tail_number, 0 AS high
 FROM plane_capacity
 WHERE (airline, tail_number) NOT IN (
    SELECT airline, tail_number
    FROM high_cap_plane))
UNION
(SELECT * FROM high_cap_plane);

-- 8.
-- Combine very_low_all, low_all,
-- fair_all, normal_all, high_all
-- to get the desired table
CREATE VIEW answer_q4 AS
SELECT very_low_all.airline AS airline,
       very_low_all.tail_number AS tail_number,
       very_low,
       low,
       fair,
       normal,
       high
FROM very_low_all JOIN low_all USING (airline, tail_number)
     JOIN fair_all USING (airline, tail_number)
     JOIN normal_all USING (airline, tail_number)
     JOIN high_all USING (airline, tail_number);

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q4 (
SELECT *
FROM answer_q4
);
