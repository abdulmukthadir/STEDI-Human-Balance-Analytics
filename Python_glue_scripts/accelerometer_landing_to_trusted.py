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
AWSGlueDataCatalog_node1783409489665 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1783409489665")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783409519450 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783409519450")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT accelerometer.*
FROM accelerometer
JOIN customer
ON accelerometer.user = customer.email
'''
SQLQuery_node1783409555703 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":AWSGlueDataCatalog_node1783409519450, "accelerometer":AWSGlueDataCatalog_node1783409489665}, transformation_ctx = "SQLQuery_node1783409555703")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783409555703, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783409093752", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783409924156 = glueContext.getSink(path="s3://abdul-stedi-2026/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783409924156")
AmazonS3_node1783409924156.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_landing_to_trusted")
AmazonS3_node1783409924156.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783409924156.writeFrame(SQLQuery_node1783409555703)
job.commit()