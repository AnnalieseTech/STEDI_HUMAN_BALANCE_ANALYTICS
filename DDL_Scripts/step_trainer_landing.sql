CREATE EXTERNAL TABLE `device_motion_raw_stream`(
    `sensor_reading_epoch` BIGINT COMMENT 'Epoch timestamp of sensor reading',
    `device_serial_id` STRING COMMENT 'Unique serial identifier of the motion sensor device',
    `object_proximity_cm` INT COMMENT 'Distance from detected object in centimeters'
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://frequently-modulated/sensor_data/motion/stream/raw/'
TBLPROPERTIES (
    'classification'='json',
    'sensor_type'='motion',
    'data_capture_method'='streaming'
);