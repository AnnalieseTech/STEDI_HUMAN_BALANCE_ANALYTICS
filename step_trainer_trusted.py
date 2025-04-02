# Import the modules required for the Glue job
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def custom_sql_transform(glue_context, sql_query, data_frames, transform_ctx) -> DynamicFrame:
    """
    Objtceive: To execute a customizable SQL query on provided DynamicFs.

    Arguments:
        glue_context (GlueContext): The Glue.
        sql_query (str): The SQL query.
        data_frames (dict): A dictionary mapping table aliases to DynamicFrames.
        transform_ctx (str): Transformation information for the result.

    Returns:
        DynamicFrame: The outcome of the SQL query as a DF.
    """
    for table_alias, frame in data_frames.items():
        frame.toDF().createOrReplaceTempView(table_alias)
    query_result = glue_context.spark_session.sql(sql_query)
    return DynamicFrame.fromDF(query_result, glue_context, transform_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Establish the custom data quality ruleset
stedi_data_quality_rules = """
    Rules = [
        ColumnCount > 0,
        "sensorreadingtime is not null",
        "serialnumber is not null",
        "distancefromobject is not null",
        "distancefromobject >= 0"
    ]
"""

# Fill data from 'step_trainer_landing' and 'customer_trusted'
step_trainer_landing_data = glue_context.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_load"
)
customer_trusted_data = glue_context.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_load"
)

# Make the SQL query to merge itself and choose relevant data
data_selection_query = """
    SELECT DISTINCT
        st.sensorreadingtime,
        st.serialnumber,
        st.distancefromobject
    FROM step_trainer AS st
    INNER JOIN customer AS cu
        ON st.serialnumber = cu.serialnumber
"""

# Dish out the custom SQL transformation
filtered_step_trainer_data = custom_sql_transform(
    glue_context=glue_context,
    sql_query=data_selection_query,
    data_frames={"customer": customer_trusted_data, "step_trainer": step_trainer_landing_data},
    transform_ctx="filtered_step_trainer_data"
)

# Assess the data quality against the customized ruleset
evaluated_data = EvaluateDataQuality().process_rows(
    frame=filtered_step_trainer_data,
    ruleset=stedi_data_quality_rules,
    publishing_options={"dataQualityEvaluationContext": "data_quality_evaluation", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Compose the transformed and evaluated data to the Glue catalog and S3
step_trainer_trusted_sink = glue_context.getSink(
    path="s3://frequently-modulated/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)
step_trainer_trusted_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
step_trainer_trusted_sink.setFormat("json")
step_trainer_trusted_sink.writeFrame(evaluated_data)

job.commit()