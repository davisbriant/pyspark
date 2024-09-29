import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.functions import get_json_object

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Getting DB credentials from Secrets Manager
client = boto3.client("secretsmanager", region_name="us-west-2")

get_secret_value_response = client.get_secret_value(
        SecretId="arkestral/psql"
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

db_username = secret.get('db_username')
db_password = secret.get('db_password')
db_url = secret.get('db_url')
output_bucket = secret.get('output_bucket')

print("Spark Version:" + spark.version)
# Connect to postgres instance and import a table containing dma_ids and their associated zip codes.
zips_query = glueContext.create_dynamic_frame.from_options ( 
 connection_type="postgresql",
 connection_options = {
 "url": db_url,
 "user": db_username,
 "password": db_password,
 "dbtable": "public.zipsindmas",
 "sampleQuery":"SELECT * FROM public.zipsindmas"
 }
)
zips_df = zips_query.toDF()
zips_df.printSchema()
# The dataframe schema contains one column dma_id of type integer and another column zips of type array. We will create a new row for every zip code in the array of zips per dma_id.
zips = zips_df.select("dma_id", explode_outer("zips").alias("zip"))
# Because some zip codes are members of multiple dmas, we de-duplicate the dataframe rows so there is a 1:1 ratio between dma_id and zip.
zips = zips.dropDuplicates(["dma_id"])
# The new schema of the dataframe contains one column dma_id of type integer and another column zip of type string.
zips.printSchema()
# now we will connect to postgres and import a table containing dma metadata
dmas_query = glueContext.create_dynamic_frame.from_options ( 
 connection_type="postgresql",
 connection_options = {
 "url": db_url,
 "user": db_username,
 "password": db_password,
 "dbtable": "public.dmametadata",
 "sampleQuery":"SELECT * FROM public.dmametadata"
 }
)
dmas_df = dmas_query.toDF()
dmas_df.printSchema()
# The dma metadata dataframe is joined with the dma zips dataframe using key dma_id.
dmas = dmas_df.join(zips, ["dma_id"], "right")
dmas.printSchema()
# Now we will connect to postgres and import a table containing data on home insurance policies.
policies_query = glueContext.create_dynamic_frame.from_options ( 
 connection_type="postgresql",
 connection_options = {
 "url": db_url,
 "user": db_username,
 "password": db_password,
 "dbtable": "public.dm_policies_extended",
 "sampleQuery":"SELECT * FROM public.dm_policies_extended"
 }
)
policies_df = policies_query.toDF()
policies_df.printSchema()
# The policies dataframe is joined with the dma metadata dataframe using (de-duplicated) key zip.
policies_bydma = policies_df.join(dmas,["zip"],"left")
policies_bydma.printSchema()
# The resulting dataframe is saved as a parquet table in the awsdatacatalog so it can be queried in Athena.

(policies_bydma
    .write
    .mode("overwrite")
    .option("path", f"s3://{output_bucket}/glue/postgres/policiesbydma/")
    .format("parquet")
    .saveAsTable("default.glue_test_postgres_policiesbydma")
)
# The data visualization team can now design dashboards that analyze policy volumes broken down by customer cohort criteria including dma.
# now let's do that again but this time we'll let postgres handle the unnesting, deplication, and joins logic
policiesbydma_query = "with dmasperzip as (select dma_id, unnest(zips) as zip2 from zipsindmas ), dmaperzip as (select *, row_number() over (partition by zip2 order by dma_id desc) as row_num from dmasperzip ), dmaperzip_metadata as (select a.zip2, b.* from dmaperzip a left join dmametadata b on a.dma_id = b.dma_id where a.row_num = 1 ) select a.*, b.* from dmaperzip_metadata a right join dm_policies_extended b on a.zip2 = b.zip" 
policiesbydma = glueContext.create_dynamic_frame.from_options ( 
 connection_type="postgresql",
 connection_options = {
 "url": db_url,
 "user": db_username,
 "password": db_password,
 "dbtable": "public.zipsindmas",
 "sampleQuery":policiesbydma_query
 }
)
# The resulting dataframe is saved as a parquet table in the awsdatacatalog so it can be queried in Athena.

(policies_bydma
    .write
    .mode("overwrite")
    .option("path", f"s3://{output_bucket}/glue/postgres/policiesbydma_alt/")
    .format("parquet")
    .saveAsTable("default.glue_test_postgres_policiesbydma_alt")
)
# The data visualization team can now design dashboards that analyze policy volumes broken down by customer cohort criteria including dma.
job.commit()