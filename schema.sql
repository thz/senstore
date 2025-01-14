create table if not EXISTS  "sensors" (
    "id" serial ,
    name VARCHAR (256),
    CONSTRAINT sensor_pkey PRIMARY Key ("id")
);

create table if not exists "sensor_readings" (
  ts TIMESTAMP not null default now(),
  sensor int CONSTRAINT sensors_foreign REFERENCES sensors(id),
  data NUMERIC(5,2)
);

