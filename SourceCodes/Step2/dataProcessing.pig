
REGISTER s3://aws-6240-class-homework-landing-zone/piggybank-0.17.0.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;


-- Setting number of reducer tasks to 10 for computation step
SET default_parallel 10;


-- Loading the input from parameter for running in Amazon S3 and EMR
weather_all_states = LOAD 's3://aws-6240-class-homework-landing-zone/all_statesData/weather_data_*.csv' using CSVLoader();

weather_lat_long = LOAD 's3://aws-6240-class-homework-landing-zone/latitude_longitude_states/statelatlong.csv' using CSVLoader();



-- parsing only required columns as weather_all_states_data
weather_all_states_data = FOREACH weather_all_states GENERATE 
$0 as name:chararray, 
$2 as tempmax:float, 
$3 as tempmin:float, 
$4 as temp:float, 
$8 as dew:float, 
$9 as humidity:float, 
$10 as precip:float, 
$14 as snow:float, 
$17 as windspeed:float, 
$19 as sealevelpressure:float, 
$20 as cloudcover:float, 
$21 as visibility:float, 
$22 as solarradiation:float, 
$24 as uvindex:int, 
$1 as datetimevalue:chararray;


lat_long_data = FOREACH weather_lat_long GENERATE
$0 as state: chararray,
$1 as latitude:float,
$2 as longitude:float,
$3 as city:chararray;


-- joining all states weather data with latitude longitude data
joined_data = JOIN weather_all_states_data BY LOWER(name), lat_long_data BY LOWER(city);



joined_data = foreach joined_data generate 
		       weather_all_states_data::name as name,  
                       weather_all_states_data::tempmax as tempmax,  
                       weather_all_states_data::tempmin as tempmin,  
                       weather_all_states_data::temp as temp,
                       weather_all_states_data::dew as dew,
 		       weather_all_states_data::humidity as humidity,  
                       weather_all_states_data::precip as precip,
                       weather_all_states_data::snow as snow,
                       weather_all_states_data::windspeed as windspeed,  
                       weather_all_states_data::sealevelpressure as sealevelpressure,
                       weather_all_states_data::cloudcover as cloudcover,
                       weather_all_states_data::visibility as visibility,  
                       weather_all_states_data::solarradiation as solarradiation,  
                       weather_all_states_data::uvindex as uvindex,
                       weather_all_states_data::datetimevalue as datetimevalue,
	               lat_long_data::state as state,
                       lat_long_data::latitude as latitude,
                       lat_long_data::longitude as longitude,
                       lat_long_data::city as city;



DUMP joined_data;

-- Storing the output to hcatalog
STORE joined_data into 'db.basetable' using org.apache.hive.hcatalog.pig.HCatStorer();

 