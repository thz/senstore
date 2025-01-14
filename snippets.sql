
select r.ts,s.name,r.data from "sensor_readings" as r INNER JOIN sensors as s ON (r.sensor = s.id) order by r.ts desc limit 20;
