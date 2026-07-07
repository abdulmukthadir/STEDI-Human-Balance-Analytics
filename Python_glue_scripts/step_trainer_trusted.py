import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783411180338 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1783411180338")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783411353036 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1783411353036")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT step.*
FROM step
WHERE step.serialNumber IN (
    SELECT serialNumber
    FROM customer
)

'''
SQLQuery_node1783411401045 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":AWSGlueDataCatalog_node1783411180338, "customer":AWSGlueDataCatalog_node1783411353036}, transformation_ctx = "SQLQuery_node1783411401045")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783411401045, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783410356693", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783411482542 = glueContext.getSink(path="s3://abdul-stedi-2026/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783411482542")
AmazonS3_node1783411482542.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1783411482542.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783411482542.writeFrame(SQLQuery_node1783411401045)
job.commit()