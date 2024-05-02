--To Set Up Database and Tables
CREATE DATABASE IF NOT EXISTS query;
USE query;

CREATE TABLE R1 (i INT, x INT);
CREATE TABLE R2 (y INT, j INT);
CREATE TABLE R3 (x INT, z INT);

--To Add Indexes for Improved Performance
CREATE INDEX idx_x_R1 ON R1(x);
CREATE INDEX idx_y_R2 ON R2(y);
CREATE INDEX idx_j_R2 ON R2(j);

--To Perform the Join Query
SELECT * FROM R1 JOIN R2 ON R1.x = R2.y JOIN R3 ON R2.j = R3.x;

SET profiling = 1;  -- Enable profiling
SELECT * FROM R1 JOIN R2 ON R1.x = R2.y JOIN R3 ON R2.j = R3.x;  -- Execute the JOIN query again if needed
SHOW PROFILES;  -- Check the timing for the query execution
