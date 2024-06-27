import os
import configparser
from pyspark.sql import SparkSession

# Set the PySpark submit arguments with the correct packages
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 pyspark-shell"

# Create the SparkSession
spark = SparkSession.builder \
    .appName("SparkMinIORead") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:39000") \
    .config("spark.hadoop.fs.s3a.access.key", "nightingale") \
    .config("spark.hadoop.fs.s3a.secret.key", "nightingale") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Define the S3 path
s3_path = "s3a://sparkbucket/data/creditfraud_20M.csv"

# Read the CSV file from S3
sparkDF = spark.read.csv(s3_path, header=True, inferSchema=True)

# Show the DataFrame content
sparkDF.show()