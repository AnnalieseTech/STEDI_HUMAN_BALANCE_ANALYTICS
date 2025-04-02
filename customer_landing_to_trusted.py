import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def filter_and_transform_data(glue_context, query_statement, input_data_frames, transform_id_label) -> DynamicFrame:
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
    for table_alias, data_frame in input_data_frames.items():
        data_frame.toDF().createOrReplaceTempView(table_alias)
    query_result = glue_context.spark_session.sql(query_statement)
    return DynamicFrame.fromDF(query_result, glue_context, transform_id_label)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_context_instance = SparkContext()
glue_context_instance = GlueContext(spark_context_instance)
spark_session_object = glue_context_instance.spark_session
job_instance_object = Job(glue_context_instance)
job_instance_object.init(args['JOB_NAME'], args)

# Specific data quality rules for filtering customer data
research_customer_data_rules = """
    Rules = [
        ColumnCount > 0,
        "serialnumber is not null",
        "registrationdate is not null",
        "sharewithresearchasofdate is not null"
    ]
"""

# Enter the landing data for customers
customer_landing_data = glue_context_instance.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_data_load"
)

# Create a SQL query that will filter clients who have provided research data
research_customer_query = """
    SELECT *
    FROM customer_data_source
    WHERE sharewithresearchasofdate IS NOT NULL
"""

# Filter and modify customer data
research_eligible_customers = filter_and_transform_data(
    glue_context=glue_context_instance,
    query_statement=research_customer_query,
    input_data_frames={"customer_data_source": customer_landing_data},
    transform_id_label="research_customer_filter"
)

# Utilize custom rules to assess the quality of the data
validated_research_customers = EvaluateDataQuality().process_rows(
    frame=research_eligible_customers,
    ruleset=research_customer_data_rules,
    publishing_options={"dataQualityEvaluationContext": "research_customer_data_validation", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Write the Glue catalog and S3 with the carefully selected customer information
research_customer_sink = glue_context_instance.getSink(
    path="s3://frequently-modulated/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="research_customer_data_sink"
)
research_customer_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
research_customer_sink.setFormat("json")
research_customer_sink.writeFrame(validated_research_customers)

job_instance_object.commit()