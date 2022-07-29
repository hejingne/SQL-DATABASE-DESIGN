-- Q3. North and South Connections

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3 (
    outbound VARCHAR(30),
    inbound VARCHAR(30),
    direct INT,
    one_con INT,
    two_con INT,
    earliest timestamp
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS city_pair CASCADE;
DROP VIEW IF EXISTS flight_on_that_day CASCADE;
DROP VIEW IF EXISTS direct CASCADE;
DROP VIEW IF EXISTS one_con_on_that_day CASCADE;
DROP VIEW IF EXISTS one_connection CASCADE;
DROP VIEW IF EXISTS two_con_on_that_day CASCADE;
DROP VIEW IF EXISTS two_connection CASCADE;
DROP VIEW IF EXISTS pairs_three_earliest_time CASCADE;
DROP VIEW IF EXISTS pairs_the_earliest_time CASCADE;
DROP VIEW IF EXISTS answer_q3 CASCADE;


-- Define views for your intermediate steps here:

-- 1.
-- Airport codes, city names of
-- City pairs between Canada and USA in both directions
CREATE VIEW city_pair AS
SELECT a1.code AS out_code,
       a2.code AS in_code,
       a1.city AS out_city,
       a2.city AS in_city
FROM Airport a1, Airport a2
WHERE a1.code <> a2.code AND (
    (a1.country = 'Canada' AND a2.country = 'USA') OR
    (a2.country = 'Canada' AND a1.country = 'USA')
    );

-- 2.
-- Outbound airport code, inbound airport code, arrival time of
-- Flights that arrive and depart on April 30, 2022
CREATE VIEW flight_on_that_day AS
SELECT outbound,
       inbound,
       s_dep,
       s_arv
FROM Flight
WHERE (s_dep >= TIMESTAMP '2022-04-30 00:00:00' AND
    s_dep < TIMESTAMP '2022-05-01 00:00:00')
    AND
    (s_arv >= TIMESTAMP '2022-04-30 00:00:00' AND
    s_arv < TIMESTAMP '2022-05-01 00:00:00');

-- 3.
-- City names, num of direct routes, earliest possible arrival time of
-- City pairs between Canada and USA in both directions
CREATE VIEW direct AS
SELECT out_city AS outbound,
       in_city AS inbound,
       count(flight_on_that_day.outbound) AS direct,
       min(s_arv) AS earliest
FROM city_pair LEFT JOIN flight_on_that_day ON
    (out_code = flight_on_that_day.outbound AND
     in_code = flight_on_that_day.inbound)
GROUP BY out_city, in_city;

-- 4.a.
-- Outbound (not connection) airport code,
-- inbound (not connection) airport code, arrival time of
-- One-connection flights that arrive and depart on April 30, 2022
CREATE VIEW one_con_on_that_day AS
SELECT f1.outbound AS outbound,
       f2.inbound AS inbound,
       f2.s_arv AS s_arv
FROM flight_on_that_day f1, flight_on_that_day f2
WHERE f1.inbound = f2.outbound AND
      f2.s_dep - f1.s_arv >= INTERVAL '30 minutes';

-- 4.b.
-- City names, num of one-connection routes, earliest possible arrival time of
-- City pairs between Canada and USA in both directions
CREATE VIEW one_connection AS
SELECT out_city AS outbound,
       in_city AS inbound,
       count(one_con_on_that_day.outbound) AS one_con,
       min(s_arv) AS earliest
FROM city_pair LEFT JOIN one_con_on_that_day ON
    (out_code = one_con_on_that_day.outbound AND
     in_code = one_con_on_that_day.inbound)
GROUP BY out_city, in_city;

-- 5.a.
-- Outbound (not connection) airport code,
-- inbound (not connection) airport code,
-- arrival time of
-- Two-connection flights that arrive and depart on April 30, 2022
CREATE VIEW two_con_on_that_day AS
SELECT f1.outbound AS outbound, f3.inbound AS inbound, f3.s_arv AS s_arv
FROM flight_on_that_day f1,
     flight_on_that_day f2,
     flight_on_that_day f3
WHERE (f1.inbound = f2.outbound AND
    f2.s_dep - f1.s_arv >= INTERVAL '30 minutes')
    AND
    (f2.inbound = f3.outbound AND
    f3.s_dep - f2.s_arv >= INTERVAL '30 minutes');

-- 5.b.
-- City names, num of two-connection routes, earliest possible arrival time of
-- City pairs between Canada and USA in both directions
CREATE VIEW two_connection AS
SELECT out_city AS outbound,
       in_city AS inbound,
       count(two_con_on_that_day.outbound) AS two_con,
       min(s_arv) AS earliest
FROM city_pair LEFT JOIN two_con_on_that_day ON
    (out_code = two_con_on_that_day.outbound AND
     in_code = two_con_on_that_day.inbound)
GROUP BY out_city, in_city;

-- 6.a.
-- City names, all three earliest time of
-- City pairs between Canada and USA in both directions
CREATE VIEW pairs_three_earliest_time AS
(SELECT * FROM direct)
UNION
(SELECT * FROM one_connection)
UNION
(SELECT * FROM two_connection);

-- 6.b.
-- City names, the earliest time of
-- City pairs between Canada and USA in both directions
CREATE VIEW pairs_the_earliest_time AS
SELECT outbound, inbound, min(earliest) AS earliest
FROM pairs_three_earliest_time
GROUP BY outbound, inbound;

-- 7.
-- Combine all 4 attributes:
--    City names, num of direct routes, num of one-con routes,
--    num of two-con routes, the earliest arrival time of
-- City pairs between Canada and USA in both directions
-- to get the desired table
CREATE VIEW answer_q3 AS
SELECT direct.outbound AS outbound,
    direct.inbound AS inbound,
    direct,
    one_con,
    two_con,
    pairs_the_earliest_time.earliest AS earliest
FROM direct, one_connection, two_connection, pairs_the_earliest_time
WHERE (direct.outbound = one_connection.outbound AND
    direct.inbound = one_connection.inbound)
    AND
    (one_connection.outbound = two_connection.outbound AND
    one_connection.inbound = two_connection.inbound)
    AND
    (two_connection.outbound = pairs_the_earliest_time.outbound AND
    two_connection.inbound = pairs_the_earliest_time.inbound);


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q3 (
SELECT outbound, inbound, direct, one_con, two_con, earliest
FROM answer_q3
);
