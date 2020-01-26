<b>Conventions to follow:</b>
1. dumping anything to database is called save. eg: saveGranularities, saveBitmap, saveSettings etc.
2. fetching anything from database is called load. eg: loadBitmap, saveSettings etc.
3. All interaction with databases will be done in CacheSystem class only. Using jdbc. 
   All rows will be either created using StructType or Bean object.
4. One table can be there corresponding to a schema type eg. power,temperature etc. \
these tables can be in different databases but only one table should be there of one schema type.
5. Didn't do proper closing of database connections...check and do the needful
6. You will need to build index manually on caching tables on columns(granularity,sensor_id,ts)
7. create a view power which avg the power from power_cache table and give user only average not sum
8. Hour level aggregation gives issue because it's aggregated value is not on starting of hour 
eg: agg(1:00-2:00) gives value of ts(1:30) while it should have given value of ts(1:00) (solve it)  


<b>view created in seil_sensor_data_v1:</b>

power: select `seil_sensor_data_v1`.`power_cache`.`granularity` AS `granularity`,`seil_sensor_data_v1`.`power_cache`.`sensor_id` AS `sensor_id`,`seil_sensor_data_v1`.`power_cache`.`ts` AS `ts`,(`seil_sensor_data_v1`.`power_cache`.`sum_power` / `seil_sensor_data_v1`.`power_cache`.`count_agg_rows`) AS `power`,`seil_sensor_data_v1`.`power_cache`.`count_agg_rows` AS `agg_row_count` from `seil_sensor_data_v1`.`power_cache`

<b>First level cache and second level cache schema available@src/main/resources/</b>
