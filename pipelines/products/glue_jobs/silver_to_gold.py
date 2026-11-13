import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set(
    "spark.sql.catalog.lakehouse",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set("spark.sql.catalog.lakehouse.type", "hadoop")
spark.conf.set(
    "spark.sql.catalog.lakehouse.warehouse",
    "s3a://ucv-data-platform/products"
)

hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://floci:4566")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.access.key", "test")
hconf.set("fs.s3a.secret.key", "test")
hconf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

silver = spark.table(SILVER_TABLE)

gold = (
    silver
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.round(F.avg("rating"), 2).alias("average_rating"),
        F.sum("stock").alias("total_stock"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price")
    )
    .withColumn("generated_at", F.current_timestamp())
    .orderBy(F.col("product_count").desc())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

(
    gold.writeTo(GOLD_TABLE)
    .using("iceberg")
    .createOrReplace()
)

spark.table(GOLD_TABLE).printSchema()
spark.table(GOLD_TABLE).show(50, truncate=False)
print(f"Gold rows: {spark.table(GOLD_TABLE).count()}")

job.commit()
