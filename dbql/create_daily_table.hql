DROP TABLE IF EXISTS climate_daily;
CREATE EXTERNAL TABLE climate_daily (
       id        STRING, 
       month 	 INT,
       day 	 INT,
       value     FLOAT )
       PARTITIONED BY (year INT)
       STORED AS PARQUET
       LOCATION '/user/cloudera/climate-2015-12-15/outdata/daily/partitions';
ALTER TABLE climate_daily ADD PARTITION (year='1971') LOCATION '/user/cloudera/climate-2015-12-15/outdata/daily/partitions/1971';
