create database db;

use db;


CREATE TABLE IF NOT EXISTS `basetable`(
  `name` string,
  `tempmax` float, 
  `tempmin` float, 
  `temp` float, 
  `dew` float, 
  `humidity` float, 
  `precip` float, 
  `snow` float, 
  `windspeed` float, 
  `sealevelpressure` float, 
  `cloudcover` float, 
  `visibility` float, 
  `solarradiation` float, 
  `uvindex` int,
  `datetimevalue` string,
  `state` string,
  `latitude` float,
  `longitude` float,
  `city` string
  )
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
TBLPROPERTIES (
  'skip.header.line.count'='1');
