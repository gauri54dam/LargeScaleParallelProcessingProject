USE db;

CREATE TABLE IF NOT EXISTS db.rawtable AS
SELECT 
    *,
    date(datetimevalue) as datevalue,
    year(datetimevalue) as yearvalue,
    month(datetimevalue) as monthvalue,
    day(datetimevalue)as dayvalue
FROM db.basetable
;


CREATE TABLE IF NOT EXISTS db.analysistable as
SELECT
    city,
    monthvalue,
    max(tempmax) as maxTemp,
    min(tempmin) as minTemp,
    avg(temp) as avgTemp
FROM
    db.rawtable
GROUP BY
    city, monthvalue
;


CREATE VIEW IF NOT EXISTS db.reportingview AS
SELECT 
    CASE 
        WHEN name IN ('Washington', 'Oregon', 'California', 'Alaska', 'Hawaii') THEN 'West Coast'
        WHEN name IN ('Maine', 'New Hampshire', 'Massachusetts', 'Rhode Island', 'Connecticut', 'New York', 'New Jersey', 'Pennsylvania', 'Delaware', 'Maryland', 'Virginia','West Virginia', 'North Carolina', 'South Carolina', 'Georgia', 'Florida', 'Vermont') THEN 'East Coast'
        WHEN name IN ('Montana', 'Idaho', 'Wyoming', 'Nevada', 'Utah', 'Colorado', 'Arizona', 'New Mexico') THEN 'Mountain'
        WHEN name IN ('Texas', 'Oklahoma', 'Arkansas', 'Louisiana', 'Mississippi', 'Alabama', 'Tennessee', 'Kentucky') THEN 'South'
        WHEN name IN ('North Dakota', 'South Dakota', 'Nebraska', 'Kansas', 'Minnesota', 'Iowa', 'Missouri', 'Wisconsin', 'Illinois', 'Indiana', 'Michigan', 'Ohio') THEN 'Central'
        ELSE 'default'
    END AS region,
    *
FROM db.basetable
;


SELECT 
  MAX(uvindex) AS maxUVindex,
  AVG(humidity) AS avgHumidity,
  AVG(precip) AS avgPrecip
FROM db.reportingview
GROUP BY region
;


