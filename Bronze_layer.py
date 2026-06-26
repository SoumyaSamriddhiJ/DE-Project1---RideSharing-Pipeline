# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np
import pandas as pd

# COMMAND ----------

spark = SparkSession.builder.appName("Bronze_layer ").getOrCreate()
bronze_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/ride_catalogue/default/ride_volume/ridesharing_raw.csv")



# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.bronze_ridesharing")


# COMMAND ----------

bronze_df.display()

# COMMAND ----------

bronze_df.printSchema()