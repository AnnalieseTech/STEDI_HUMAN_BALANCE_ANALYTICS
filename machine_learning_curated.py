# Import necessary libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def harmonize_sensor_data(glue_ctx, sql_statement, frame_mapping, transform_id) -> DynamicFrame:
    """
    Objective: Build a custom SQL query to join and select data from multiple DynamicFrames.

    Arguments:
        glue_ctx (GlueContext): The Glue.
        sql_statement (str): The SQL query dealing with data transformation.
        frame_mapping (dict): A dictionary linking table aliases to DFs.
        transform_id (str): Transformation identifier.

    Returns:
        DynamicFrame: The outcome of the SQL query as a DF.
    """
    for table_alias, data_frame in frame_mapping.items():
        data_frame.toDF().createOrReplaceTempView(table_alias)
    query_result = glue_ctx.spark_session.sql(sql_statement)
    return DynamicFrame.fromDF(query_result, glue_ctx, transform_id)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
job_instance = Job(glue_context)
job_instance.init(args['JOB_NAME'], args)

# Customize rule set for data quality evaluation
sensor_data_quality_rules = """
    Rules = [
        ColumnCount > 0,
        "timestamp is not null",
        "sensorreadingtime is not null",
        "x is not null",
        "y is not null",
        "z is not null"
    ]
"""

# Work the accelerometer and step trainer trusted information
accelerometer_data = glue_context.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_data_load"
)
step_trainer_data = glue_context.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_data_load"
)

# Build the SQL query to merge accelerometer and step trainer data
sensor_data_join_query = """
    SELECT
        accel.*,
        step.distancefromobject
    FROM accel
    INNER JOIN step
        ON accel.timestamp = step.sensorreadingtime
"""

# Use the custom function to deal with the sensor data
synchronized_sensor_data = harmonize_sensor_data(
    glue_ctx=glue_context,
    sql_statement=sensor_data_join_query,
    frame_mapping={"accel": accelerometer_data, "step": step_trainer_data},
    transform_id="sensor_data_harmonization"
)

# Make use of the custom ruleset to assess the quality of data
validated_sensor_data = EvaluateDataQuality().process_rows(
    frame=synchronized_sensor_data,
    ruleset=sensor_data_quality_rules,
    publishing_options={"dataQualityEvaluationContext": "sensor_data_validation", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Transfer the verified and standardized data to the Glue catalog and S3. 
ml_curated_sink = glue_context.getSink(
    path="s3://frequently-modulated/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="ml_curated_data_sink"
)
ml_curated_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
ml_curated_sink.setFormat("json")
ml_curated_sink.writeFrame(validated_sensor_data)

job_instance.commit()