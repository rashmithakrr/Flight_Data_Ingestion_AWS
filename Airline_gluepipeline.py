import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node dim_airports
dim_airports_node1751693760309 = glueContext.create_dynamic_frame.from_catalog(
    database="airline_datamart", table_name="dev_airlines_airports_dim", redshift_tmp_dir="s3://aws-glue-assets-<<aws-acct-id>>-us-east-1/temporary/", transformation_ctx="dim_airports_node1751693760309")

# Script generated for node flight_data
flight_data_node1751693289273 = glueContext.create_dynamic_frame.from_catalog(
    database="airline_datamart", table_name="dailyflights", transformation_ctx="flight_data_node1751693289273")

# Script generated for node Delayfilter
Delayfilter_node1751693550546 = Filter.apply(frame=flight_data_node1751693289273, f=lambda row: (
    row["depdelay"] >= 60), transformation_ctx="Delayfilter_node1751693550546")

# Script generated for node origin_airport
Delayfilter_node1751693550546DF = Delayfilter_node1751693550546.toDF()
dim_airports_node1751693760309DF = dim_airports_node1751693760309.toDF()
origin_airport_node1751693835925 = DynamicFrame.fromDF(Delayfilter_node1751693550546DF.join(dim_airports_node1751693760309DF, (
    Delayfilter_node1751693550546DF['originairportid'] == dim_airports_node1751693760309DF['airport_id']), "left"), glueContext, "origin_airport_node1751693835925")

# Script generated for node origin_table_schema
origin_table_schema_node1751694003345 = ApplyMapping.apply(frame=origin_airport_node1751693835925, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), (
    "arrdelay", "long", "arr_delay", "bigint"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="origin_table_schema_node1751694003345")

# Script generated for node arrival_join
origin_table_schema_node1751694003345DF = origin_table_schema_node1751694003345.toDF()
dim_airports_node1751693760309DF = dim_airports_node1751693760309.toDF()
arrival_join_node1751694245152 = DynamicFrame.fromDF(origin_table_schema_node1751694003345DF.join(dim_airports_node1751693760309DF, (
    origin_table_schema_node1751694003345DF['destairportid'] == dim_airports_node1751693760309DF['airport_id']), "left"), glueContext, "arrival_join_node1751694245152")

# Script generated for node arrival_schema
arrival_schema_node1751694584962 = ApplyMapping.apply(frame=arrival_join_node1751694245152, mappings=[("dep_delay", "bigint", "dep_delay", "bigint"), ("arr_delay", "bigint", "arr_delay", "bigint"), ("carrier", "string", "carrier", "string"), ("dep_city", "string", "dep_city", "string"), (
    "dep_airport", "string", "dep_airport", "string"), ("dep_state", "string", "dep_state", "string"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("state", "string", "arr_state", "string")], transformation_ctx="arrival_schema_node1751694584962")

# Script generated for node flights_fact_target
flights_fact_target_node1751694848643 = glueContext.write_dynamic_frame.from_options(frame=arrival_schema_node1751694584962, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-<<aws-acct-id>>-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.daily_flights_fact",
                                                                                     "connectionName": "Redshift connection1", "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (dep_delay VARCHAR, arr_delay VARCHAR, carrier VARCHAR, dep_city VARCHAR, dep_airport VARCHAR, dep_state VARCHAR, arr_city VARCHAR, arr_airport VARCHAR, arr_state VARCHAR);"}, transformation_ctx="flights_fact_target_node1751694848643")

job.commit()
