import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node AWS Glue Data Catalog Order
AWSGlueDataCatalogOrder_node1757410364062 = glueContext.create_dynamic_frame.from_catalog(database="demo-glue-s3-db", table_name="orders_csv", transformation_ctx="AWSGlueDataCatalogOrder_node1757410364062")

# Script generated for node AWS Glue Data Catalog Customer
AWSGlueDataCatalogCustomer_node1757410238906 = glueContext.create_dynamic_frame.from_catalog(database="demo-glue-s3-db", table_name="customers_csv", transformation_ctx="AWSGlueDataCatalogCustomer_node1757410238906")

# Script generated for node Change Schema
ChangeSchema_node1757410638701 = ApplyMapping.apply(frame=AWSGlueDataCatalogOrder_node1757410364062, mappings=[("order_id", "long", "order_id", "long"), ("customer_id", "long", "customer_id", "long"), ("order_date", "string", "order_date", "date"), ("total_amount", "double", "total_amount", "double"), ("status", "string", "status", "string")], transformation_ctx="ChangeSchema_node1757410638701")

# Script generated for node Change Schema
ChangeSchema_node1757410074709 = ApplyMapping.apply(frame=AWSGlueDataCatalogCustomer_node1757410238906, mappings=[("customer_id", "long", "customer_id", "string"), ("name", "string", "name", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("country", "string", "country", "string"), ("registration_date", "string", "registration_date", "date"), ("is_active", "boolean", "is_active", "string")], transformation_ctx="ChangeSchema_node1757410074709")

# Script generated for node Renamed keys for Customer_order_Join
RenamedkeysforCustomer_order_Join_node1757410910452 = ApplyMapping.apply(frame=ChangeSchema_node1757410074709, mappings=[("customer_id", "string", "right_customer_id", "string"), ("name", "string", "right_name", "string"), ("city", "string", "right_city", "string"), ("state", "string", "right_state", "string"), ("country", "string", "right_country", "string"), ("registration_date", "date", "right_registration_date", "date"), ("is_active", "string", "right_is_active", "string")], transformation_ctx="RenamedkeysforCustomer_order_Join_node1757410910452")

# Script generated for node Customer_order_Join
ChangeSchema_node1757410638701DF = ChangeSchema_node1757410638701.toDF()
RenamedkeysforCustomer_order_Join_node1757410910452DF = RenamedkeysforCustomer_order_Join_node1757410910452.toDF()
Customer_order_Join_node1757410076221 = DynamicFrame.fromDF(ChangeSchema_node1757410638701DF.join(RenamedkeysforCustomer_order_Join_node1757410910452DF, (ChangeSchema_node1757410638701DF['customer_id'] == RenamedkeysforCustomer_order_Join_node1757410910452DF['right_customer_id']), "right"), glueContext, "Customer_order_Join_node1757410076221")

# Script generated for node Customer_order_Aggregate
Customer_order_Aggregate_node1757410124564 = sparkAggregate(glueContext, parentFrame = Customer_order_Join_node1757410076221, groups = ["customer_id"], aggs = [["order_id", "count"]], transformation_ctx = "Customer_order_Aggregate_node1757410124564")

# Script generated for node Output-data in S3
EvaluateDataQuality().process_rows(frame=Customer_order_Aggregate_node1757410124564, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757410046155", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
OutputdatainS3_node1757410148306 = glueContext.getSink(path="s3://demo-bucket-2604/output-data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="OutputdatainS3_node1757410148306")
OutputdatainS3_node1757410148306.setCatalogInfo(catalogDatabase="demo-glue-s3-db",catalogTableName="output-table")
OutputdatainS3_node1757410148306.setFormat("glueparquet", compression="snappy")
OutputdatainS3_node1757410148306.writeFrame(Customer_order_Aggregate_node1757410124564)
job.commit()