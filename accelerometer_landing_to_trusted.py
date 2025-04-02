import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def extract_and_transform_data(glue_context, sql_command, input_tables, transformation_label) -> DynamicFrame:
    """
   Applies a transformation and a SQL filter to the supplied DynamicFrames

    Args:
        glue_context (GlueContext): The Glue.
        query_statement (str): The filtering and transformation SQL query. 
        input_data_frames (dict): A dictionary mapping table that aliases to DynamicFrames
        transform_id_label (str): Transformation identifier.

    Returns:
        DynamicFrame: The outcome of the SQL query as a DynamicFrame.
    """
    for table_alias, data_frame in input_tables.items():
        data_frame.toDF().createOrReplaceTempView(table_alias)
    query_result = glue_context.spark_session.sql(sql_command)
    return DynamicFrame.fromDF(query_result, glue_context, transformation_label)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_context_instance = SparkContext()
glue_context_object = GlueContext(spark_context_instance)
spark_session_instance = glue_context_object.spark_session
job_instance = Job(glue_context_object)
job_instance.init(args['JOB_NAME'], args)

# Customized DQ rules for accelerometer data
accelerometer_data_quality_rules = """
    Rules = [
        ColumnCount > 0,
        "x is not null",
        "y is not null",
        "z is not null",
        "user is not null",
        "timestamp is not null"
    ]
"""

# Add customer-trusted data and accelerometer landing
accelerometer_landing_data = glue_context_object.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_load"
)
customer_trusted_data = glue_context_object.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_load"
)

# Combine accelerometer and customer data according to user and email
joined_customer_accelerometer_data = Join.apply(
    frame1=customer_trusted_data,
    frame2=accelerometer_landing_data,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_accelerometer_join"
)

# Build a SQL query to take out relevant accelerometer data
accelerometer_data_extraction_query = """
    SELECT
        x,
        y,
        z,
        user,
        timestamp
    FROM joined_data_source
"""

# Extract and modify data from accelerometers
selected_accelerometer_data = extract_and_transform_data(
    glue_context=glue_context_object,
    sql_command=accelerometer_data_extraction_query,
    input_tables={"joined_data_source": joined_customer_accelerometer_data},
    transformation_label="accelerometer_data_selection"
)

# Assess data quality using custom criteria
validated_accelerometer_data = EvaluateDataQuality().process_rows(
    frame=selected_accelerometer_data,
    ruleset=accelerometer_data_quality_rules,
    publishing_options={"dataQualityEvaluationContext": "accelerometer_data_validation", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Compose the curated accelerometer data to the Glue catalog and S3
accelerometer_trusted_sink = glue_context_object.getSink(
    path="s3://frequently-modulated/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_data_sink"
)
accelerometer_trusted_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
accelerometer_trusted_sink.setFormat("json")
accelerometer_trusted_sink.writeFrame(validated_accelerometer_data)

job_instance.commit()