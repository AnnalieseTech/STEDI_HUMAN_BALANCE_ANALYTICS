import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def transform_and_query_data(glue_context, sql_command, input_frames, transform_label) -> DynamicFrame:
    """
    Converts DynamicFrames and runs a SQL query on them. 

    Args:
        glue_context (GlueContext): The Glue.
        sql_command (str): The data selection SQL query.
        input_frames (dict): DynamicFrames is an alias for a dictionary mapping table.
        transform_label (str): The context identifier for the transformation.

    Returns:
        DynamicFrame: The SQL query's outcome.
    """
    for table_alias, data_frame in input_frames.items():
        data_frame.toDF().createOrReplaceTempView(table_alias)
    query_result = glue_context.spark_session.sql(sql_command)
    return DynamicFrame.fromDF(query_result, glue_context, transform_label)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark_sess = glue_ctx.spark_session
job_obj = Job(glue_ctx)
job_obj.init(args['JOB_NAME'], args)

# Customize data quality rules on customer data
customer_data_quality_rules = """
    Rules = [
        ColumnCount > 0,
        "serialnumber is not null",
        "registrationdate is not null"
    ]
"""

# Load trusted customer data and accelerometer data
accelerometer_data = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_data_load"
)
customer_data = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_data_load"
)

# Connect customer and accelerometer data using email and user information
joined_data = Join.apply(
    frame1=accelerometer_data,
    frame2=customer_data,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="data_join"
)

# Create a SQL query to choose particular client information
customer_data_query = """
    SELECT DISTINCT
        serialnumber,
        registrationdate,
        lastupdatedate,
        sharewithresearchasofdate,
        sharewithpublicasofdate,
        sharewithfriendsasofdate
    FROM joined_data
"""

# Convert and query the combined data
selected_customer_data = transform_and_query_data(
    glue_ctx=glue_ctx,
    sql_command=customer_data_query,
    input_frames={"joined_data": joined_data},
    transform_label="customer_data_selection"
)

# Utilize custom rules to assess the quality of the data
validated_customer_data = EvaluateDataQuality().process_rows(
    frame=selected_customer_data,
    ruleset=customer_data_quality_rules,
    publishing_options={"dataQualityEvaluationContext": "customer_data_validation", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Upload the curated client information to the Glue catalog and S3
curated_customer_sink = glue_ctx.getSink(
    path="s3://frequently-modulated/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="curated_customer_