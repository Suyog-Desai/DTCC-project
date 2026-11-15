"""One place that builds the Spark session, so every job uses the same settings.

get_spark("glue", bucket=...)  -> Iceberg tables registered in the AWS Glue Data Catalog,
                                  data and metadata stored in s3://<bucket>/warehouse
get_spark("local")             -> Iceberg tables in a local folder (no AWS), for trying things out
"""
from pyspark.sql import SparkSession

CATALOG = "lake"                      # the name we use in SQL: lake.taxi_db.trips_clean
ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
ICEBERG_AWS = "org.apache.iceberg:iceberg-aws-bundle:1.9.2"   # S3FileIO + Glue client
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"             # lets Spark read s3a:// paths


def get_spark(kind: str = "glue", bucket: str = None, region: str = "us-east-2",
              local_warehouse: str = "warehouse_local") -> SparkSession:
    packages = [ICEBERG]
    b = (
        SparkSession.builder.appName("Taxi pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        # needed for MERGE INTO / ALTER TABLE ... on Iceberg tables
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    )

    if kind == "glue":
        if not bucket:
            raise ValueError("bucket is required for the glue catalog")
        packages += [ICEBERG_AWS, HADOOP_AWS]
        b = (
            b.config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{bucket}/warehouse")
            .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{CATALOG}.client.region", region)
            # s3a:// is only used to READ the raw files and WRITE the rejects.
            # DefaultAWSCredentialsProviderChain understands AWS_PROFILE and ~/.aws/credentials.
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        )
    elif kind == "local":
        b = (
            b.config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", local_warehouse)
        )
    else:
        raise ValueError(f"unknown catalog kind: {kind}")

    spark = b.config("spark.jars.packages", ",".join(packages)).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
