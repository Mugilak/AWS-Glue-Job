import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
hi
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node MySQL
MySQL_node1705647779038 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "schema.Employee",
        "connectionName": "mysql jdbc connection",
    },
    transformation_ctx="MySQL_node1705647779038",
)

# Script generated for node Select Fields
SelectFields_node1705647890941 = SelectFields.apply(
    frame=MySQL_node1705647779038,
    paths=[],
    transformation_ctx="SelectFields_node1705647890941",
)

# Script generated for node Amazon S3
AmazonS3_node1705647937432 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1705647890941,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://output-data-covid", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1705647937432",
)

job.commit()
