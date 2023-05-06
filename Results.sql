select count(*) from idrac10.test_voltagereading 
--436839101
select count(*) from idrac10.schemed_voltagereading 
--303754
--Voulme Reduction -99.93%

select count(*) from idrac10.test_temperaturereading
--57080186
select count(*) from idrac10.schemed_temperaturereading 
--535684
--Voulme Reduction -99.06%

select count(*) from idrac10.test_systempowerconsumption
--19904286
select count(*) from idrac10.schemed_systempowerconsumption
--1088124
--Voulme Reduction - 94.5%


explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value    
FROM idrac10.test_voltagereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 M01 VTT PG'
and nodeid = '467'
GROUP BY time, nodeid, label
ORDER BY time;
--Execution Time: 7585.343 ms
explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value, duplicate_timestamps     
FROM idrac10.schemed_voltagereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 M01 VTT PG'
and nodeid = '467'
GROUP BY time, nodeid, label, duplicate_timestamps
ORDER BY time;
--Execution Time: 28.734 ms
explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value     
FROM idrac10.test_temperaturereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 Temp'
and nodeid = '4'
GROUP BY time, nodeid, label
ORDER BY time;
--Execution Time: 1336.643 ms
explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value, duplicate_timestamps     
FROM idrac10.schemed_temperaturereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 Temp'
and nodeid = '4'
GROUP BY time, nodeid, label, duplicate_timestamps
ORDER BY time;
--Execution Time: 64.155 ms
explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value     
FROM idrac10.test_systempowerconsumption
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'System Power Control'
and nodeid = '466'
GROUP BY time, nodeid, label
ORDER BY time;
--Execution Time: 579.446 ms
explain analyse SELECT time_bucket_gapfill('1 day', timestamp) AS time, nodeid, fqdd AS label,max(value) AS value, duplicate_timestamps     
FROM idrac10.schemed_systempowerconsumption
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'System Power Control'
and nodeid = '466'
GROUP BY time, nodeid, label, duplicate_timestamps
ORDER BY time;
--Execution Time: 102.609 ms

explain analyse select * 
FROM idrac10.test_voltagereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 M01 VTT PG'
and nodeid = '466'
--Execution Time: 7786.764 ms
explain analyse select * 
FROM idrac10.schemed_voltagereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 M01 VTT PG'
and nodeid = '466'
--Execution Time: 27.753 ms
explain analyse SELECT *  
FROM idrac10.test_temperaturereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 Temp'
and nodeid = '45'
--Execution Time: 1364.976 ms
explain analyse SELECT *  
FROM idrac10.schemed_temperaturereading
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'CPU1 Temp'
and nodeid = '45'
--Execution Time: 61.986 ms
explain analyse SELECT *
FROM idrac10.test_systempowerconsumption
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'System Power Control'
and nodeid = '46'
--Execution Time: 652.137 ms
explain analyse SELECT *
FROM idrac10.schemed_systempowerconsumption
WHERE timestamp >= '2022-09-01'
AND timestamp < '2022-09-30'      
AND fqdd = 'System Power Control'
and nodeid = '46'
--Execution Time: 85.510 ms