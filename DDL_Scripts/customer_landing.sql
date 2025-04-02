CREATE EXTERNAL TABLE `user_profile_initial_intake`(
    `full_name` STRING COMMENT 'Complete name of the user',
    `contact_email` STRING COMMENT 'User\'s primary email address',
    `contact_phone` STRING COMMENT 'User\'s contact phone number',
    `date_of_birth` STRING COMMENT 'User\'s birth date in string format',
    `device_serial_id` STRING COMMENT 'Unique serial identifier of the user\'s device',
    `account_creation_timestamp` BIGINT COMMENT 'Epoch timestamp of account registration',
    `last_profile_update_timestamp` BIGINT COMMENT 'Epoch timestamp of last profile modification',
    `research_consent_timestamp` BIGINT COMMENT 'Epoch timestamp of consent to share data for research',
    `public_share_consent_timestamp` BIGINT COMMENT 'Epoch timestamp of consent to share data publicly',
    `friend_share_consent_timestamp` BIGINT COMMENT 'Epoch timestamp of consent to share data with friends'
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://frequently-modulated/user_data/profiles/raw/'
TBLPROPERTIES (
    'classification'='json',
    'source_system'='mobile_app_registration',
    'data_category'='user_profiles'
);