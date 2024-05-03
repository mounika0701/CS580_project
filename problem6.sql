-- Database named p580;
USE p580;

-- Creating Relations R1, R2 and R3
CREATE TABLE R1(a int,b int);
CREATE TABLE R2(c int,d int);
CREATE TABLE R3(x int,y int);

-- Line Query Implementation
SELECT a,b,d,y FROM R1
JOIN R2 ON R1.b = R2.c
JOIN R3 ON R2.d = R3.x;
