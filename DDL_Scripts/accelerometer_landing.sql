CREATE EXTERNAL TABLE `sensor_acceleration_raw`(
    `device_user_id` STRING COMMENT 'Unique user identifier from sensor device',
    `recording_timestamp` BIGINT COMMENT 'Epoch timestamp of acceleration recording',
    `axis_x_acceleration` FLOAT COMMENT 'Acceleration along the X-axis',
    `axis_y_acceleration` FLOAT COMMENT 'Acceleration along the Y-axis',
    `axis_z_acceleration` FLOAT COMMENT 'Acceleration along the Z-axis'
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://frequently-modulated/sensor_data/acceleration/raw/'
TBLPROPERTIES (
    'classification'='json',
    'source_data'='sensor_device',
    'data_type'='acceleration'
);