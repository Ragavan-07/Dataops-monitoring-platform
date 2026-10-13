"""SparkSession creation and the tuning switches used by the benchmark."""

import os

from pyspark.sql import SparkSession

from pipeline import config

# hadoop-aws must match the Hadoop version bundled with PySpark 3.5.x (Hadoop 3.3.4).
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_spark(app_name: str) -> SparkSession:
    config.validate_storage_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        # Overwrite only the date partitions being written, not the whole table.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.STORAGE_MODE == "s3":
        builder = (
            builder.config("spark.jars.packages", HADOOP_AWS_PACKAGE)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config.AWS_REGION}.amazonaws.com")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_tuning(spark, optimized=True)
    return spark


def configure_tuning(spark: SparkSession, optimized: bool) -> None:
    """Optimized: few shuffle partitions + adaptive query execution.
    Baseline: Spark's untuned defaults (200 shuffle partitions, AQE off)."""
    if optimized:
        spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
    else:
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        spark.conf.set("spark.sql.adaptive.enabled", "false")
