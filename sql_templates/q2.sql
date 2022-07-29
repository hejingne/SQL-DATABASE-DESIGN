-- Q2. Refunds!

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel;
DROP TABLE IF EXISTS q2 CASCADE;

CREATE TABLE q2 (
    airline CHAR(2),
    name VARCHAR(50),
    year CHAR(4),
    seat_class seat_class,
    refund REAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS delayed_flight CASCADE;
DROP VIEW IF EXISTS delayed_no_refund_flight CASCADE;
DROP VIEW IF EXISTS refund_flight CASCADE;
DROP VIEW IF EXISTS flight_and_country CASCADE;
DROP VIEW IF EXISTS small_refund_flight CASCADE;
DROP VIEW IF EXISTS big_refund_flight CASCADE;
DROP VIEW IF EXISTS flight_year_price_seat CASCADE;
DROP VIEW IF EXISTS small_refund_group CASCADE;
DROP VIEW IF EXISTS big_refund_group CASCADE;
DROP VIEW IF EXISTS both_refund_group CASCADE;
DROP VIEW IF EXISTS answer_q2 CASCADE;


-- Define views for your intermediate steps here:

-- 1.a.
-- Id of
-- Flights that had departure delays
CREATE VIEW delayed_flight AS
SELECT id
FROM Flight JOIN Departure ON id = flight_id
WHERE datetime > s_dep;

-- 1.b.
-- Id of
-- Flights that DON'T need to refund because the pilots managed to make up time
CREATE VIEW delayed_no_refund_flight AS
SELECT id
FROM Flight, Departure dep, Arrival arv
WHERE (id = dep.flight_id AND id = arv.flight_id) AND
    dep.datetime > s_dep AND
    arv.datetime > s_arv AND
    arv.datetime - s_arv <= 0.5 * (dep.datetime - s_dep)
;

-- 2.
-- Id of
-- Flights that need to refund
CREATE VIEW refund_flight AS
(SELECT * FROM delayed_flight)
EXCEPT
(SELECT * FROM delayed_no_refund_flight);

-- 3.
-- Id, outbound country, inbound country, s_dep, s_arv of
-- Flights
CREATE VIEW flight_and_country AS
SELECT id, a1.country AS o_country,
       a2.country AS i_country,
       s_dep,
       s_arv
FROM Flight, Airport a1, Airport a2
WHERE outbound = a1.code AND inbound = a2.code;

-- 4.
-- Id of
-- Flights (both domestic and international) that should refund 35%
CREATE VIEW small_refund_flight AS
SELECT id
FROM refund_flight
WHERE EXISTS (
SELECT *
FROM flight_and_country JOIN Departure ON flight_and_country.id = flight_id
WHERE refund_flight.id = flight_and_country.id AND (
    (o_country = i_country AND     -- domestic flight
     datetime - s_dep >= INTERVAL '5 hours' AND
     datetime - s_dep < INTERVAL '10 hours')
     OR
    (o_country <> i_country AND    -- international flight
    datetime - s_dep >= INTERVAL '8 hours'
    AND datetime - s_dep < INTERVAL '12 hours')
    )
);

-- 5.
-- Id of
-- Flights (both domestic and international) that should refund 50%
CREATE VIEW big_refund_flight AS
SELECT id
FROM refund_flight
WHERE EXISTS (
SELECT *
FROM flight_and_country JOIN Departure ON flight_and_country.id = flight_id
WHERE refund_flight.id = flight_and_country.id AND (
    (o_country = i_country AND -- domestic flight
    datetime - s_dep >= INTERVAL '10 hours')
    OR
    (o_country <> i_country AND -- international flight
    datetime - s_dep >= INTERVAL '12 hours')
    )
);

-- 6.
-- Flight_id, year, seat_class, price of
-- Flights
CREATE VIEW flight_year_price_seat AS
SELECT flight_id,
       EXTRACT(YEAR FROM s_dep) AS year,
       price,
       seat_class
FROM Flight JOIN Booking ON Flight.id = Booking.flight_id;

-- 7.
-- Flight_id, year, seat_class, refund of
-- Flights that should refund 35%
CREATE VIEW small_refund_group AS
SELECT flight_id,
       year,
       0.35 * sum(price) AS refund,
       seat_class
FROM flight_year_price_seat
WHERE flight_id in (
    SELECT id
    FROM small_refund_flight
)
GROUP BY flight_id, year, seat_class;

-- 8.
-- Flight_id, year, seat_class, refund of
-- Flights that should refund 50%
CREATE VIEW big_refund_group AS
SELECT flight_id,
       year,
       0.5 * sum(price) AS refund,
       seat_class
FROM flight_year_price_seat
WHERE flight_id in (
    SELECT id
    FROM big_refund_flight
)
GROUP BY flight_id, year, seat_class;

-- 9.
-- Combine both small and big refund groups
CREATE VIEW both_refund_group AS
(SELECT * FROM small_refund_group)
UNION
(SELECT * FROM big_refund_group);

-- 10.
-- Combine with info re. airline code and name to get desired table
CREATE VIEW answer_q2 AS
SELECT code AS airline,
       name,
       year,
       seat_class,
       sum(refund) AS refund
FROM both_refund_group JOIN Flight ON flight_id = id
    JOIN Airline ON airline = code
GROUP BY code, name, year, seat_class;


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q2 (
SELECT airline, name, year, seat_class, refund
FROM answer_q2
);
