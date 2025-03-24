CREATE DATABASE IF NOT EXISTS movies_pbd_projekt1;
USE movies_pbd_projekt1;

ADD JAR hdfs:///user/${USER}/project_files/hive-hcatalog-core-2.3.7.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_RATES(
movie_id INT,
count INT,
sum_rate INT)
COMMENT 'Movie rates'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.mapred.SequenceFileOutputFormat'
location '/user/${USER}/output_mr3';

CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_TITLES(
id INT,
year INT,
title STRING)
COMMENT 'Movie titles'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/${USER}/input/datasource4';

CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_RESULT(
year INT,
title STRING,
average DOUBLE)
COMMENT 'Movie result'
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
location '/user/${USER}/output6';

INSERT INTO movie_result 
SELECT f.year, f.title, f.average FROM (SELECT t.year, t.title, a.average, RANK () OVER (PARTITION BY t.year ORDER BY a.average DESC) rank_in_year 
FROM (SELECT movie_id, sum_rate/count as average FROM movie_rates WHERE count > 1000) a JOIN movie_titles t ON a.movie_id=t.id WHERE t.year is not null) f 
WHERE f.rank_in_year <= 3;