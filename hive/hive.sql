# You sql follows

CREATE EXTERNAL TABLE IF NOT EXISTS escuelasPR(regionDeEscuela STRING, distritoDescuela STRING, ciudad STRING, schoolid INT, schoolname STRING,nivel STRING , CBID INT)
COMMENT 'schools of pr'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/joanette_rosario/csvesc';

CREATE EXTERNAL TABLE IF NOT EXISTS estudiantesPR(regionDeEscuela STRING, distritoDescuela STRING, schoolid INT, schoolname STRING, nivel STRING, sexo STRING, studentid INT)
COMMENT 'schools of pr'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/joanette_rosario/csv';

CREATE TABLE jointable AS
SELECT * FROM
(
SELECT * FROM estudiantesPR as ePR 
INNER JOIN escuelasPR as esc on ePR.schoolid = esc.schoolid
)AS NewTable;

SELECT regionDeEscuela, ciudad, COUNT(*)
FROM jointable
GROUP BY regionDeEscuela, ciudad;

SELECT ciudad, nivel, count(*)
FROM escuelasPR
GROUP BY nivel, ciudad;


SELECT ciudad, nivel , sexo, COUNT(*) 
FROM jointable
WHERE sexo = 'F' 
AND nivel = 'Superior'
AND ciudad = 'Ponce'
GROUP BY ciudad, nivel, sexo;

SELECT regionDeEscuela, distritoDescuela, ciudad, sexo, count(*)
FROM jointable 
WHERE sexo = 'M' 
GROUP BY  regionDeEscuela, distritoDescuela, ciudad, sexo;