from pyspark.sql import SparkSession

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("ucv-query-iceberg")

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "hadoop"
        )
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            "s3a://ucv-data-platform/products"
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://floci:4566"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "test"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "test"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .getOrCreate()
    )


spark = build_spark()

print("\n=== SILVER ===")
spark.table(SILVER_TABLE).printSchema()
spark.table(SILVER_TABLE).show(20, truncate=False)

print(
    "Silver rows:",
    spark.table(SILVER_TABLE).count()
)

print("\n=== GOLD ===")
spark.table(GOLD_TABLE).printSchema()
spark.table(GOLD_TABLE).show(50, truncate=False)

print(
    "Gold rows:",
    spark.table(GOLD_TABLE).count()
)

spark.stop()