# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np
import pandas as pd
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col




# COMMAND ----------

spark = SparkSession.builder.appName("Silver_layer").getOrCreate()
silver_df = spark.read.format("delta").table("ride_catalogue.default.bronze_ridesharing")

# COMMAND ----------

silver_df = silver_df.dropDuplicates(["trip_id"])


# COMMAND ----------

# DBTITLE 1,Cell 4
silver_df = silver_df.dropna(subset=["trip_id","rider_id","driver_id"])
silver_df = silver_df.fillna({"cancellation_reason":"No_Reason",
                              "promo_code":"No_Promo",
                              "promo_discount":0,
                              "os_platform":"Unknown",
                              "rider_name":"Unknown",
                              "rider_email":"Unknown",
                              "payment_type":"Unknown",
                              "surge_zone":"Unknown",
                              "driver_name":"Unknown",
                              "rider_phone":"Unknown",
                              "driver_phone":"Unknown",
                              "pickup_location":"Unknown",
                              "dropoff_location":"Unknown",
                              "vehicle_type":"Unknown",
                              "vehicle_plate":"Unknown",
                              "ride_status": "Unknown",
                              "dropoff_datetime":"Unknown",
                              "pickup_datetime":"Unknown",
                              "tip": 0
                              })

# COMMAND ----------

silver_df = silver_df.filter((col("total_fare")>0)&(col("distance_km")>0)&(col("trip_duration_mins")>0))

# COMMAND ----------

silver_df = silver_df.withColumn("vehicle_type",lower(col("vehicle_type")))\
                     .withColumn("payment_type",lower(col("payment_type")))\
                     .withColumn("ride_status",lower(col("ride_status")))

# COMMAND ----------

silver_df = silver_df.filter(col("driver_rating").between(1,5)&(col("rider_rating").between(1,5)))

# COMMAND ----------

# silver_df = silver_df.withColumn("pickup_hour",hour(col("pickup_datetime")))\
#                      .withColumn("pickup_date",to_date(col("pickup_datetime")))


# COMMAND ----------

# DBTITLE 1,Cell 10
pickup_lat_mean  = silver_df.select(mean("pickup_lat")).collect()[0][0]
pickup_lng_mean  = silver_df.select(mean("pickup_lng")).collect()[0][0]
dropoff_lat_mean = silver_df.select(mean("dropoff_lat")).collect()[0][0]
dropoff_lng_mean = silver_df.select(mean("dropoff_lng")).collect()[0][0]
silver_df = silver_df.fillna({"pickup_lat":pickup_lat_mean,"pickup_lng":pickup_lng_mean,"dropoff_lat":dropoff_lat_mean,"dropoff_lng":dropoff_lng_mean})


# COMMAND ----------

silver_df = silver_df.withColumn("pickup_lat", round(col("pickup_lat"), 6))
silver_df = silver_df.withColumn("pickup_lng", round(col("pickup_lng"), 6))
silver_df = silver_df.withColumn("dropoff_lat", round(col("dropoff_lat"), 6))
silver_df = silver_df.withColumn("dropoff_lng", round(col("dropoff_lng"), 6))

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.dtypes

# COMMAND ----------


# silver_df.select([count(when(col(c).isNull(), c)).alias(c) for c in silver_df.columns]).display()

# COMMAND ----------

# DBTITLE 1,Cell 17
base_fare_median = float(silver_df.approxQuantile("base_fare", [0.5], 0.0)[0])
surge_multiplier_mean = float(silver_df.select(mean("surge_multiplier")).collect()[0][0])

silver_df = silver_df.fillna({"base_fare": base_fare_median, "surge_multiplier": surge_multiplier_mean})

# COMMAND ----------

silver_df = silver_df.withColumn("surge_multiplier", round(col("surge_multiplier"), 1))


# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.display()

# COMMAND ----------

silver_df = silver_df.withColumn("surge_zone", lower(col("surge_zone"))) \
                     .withColumn("pickup_location", lower(col("pickup_location"))) \
                     .withColumn("dropoff_location", lower(col("dropoff_location"))) \
                     .withColumn("os_platform", lower(col("os_platform"))) \
                     .withColumn("promo_code", lower(col("promo_code")))

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.silver_ridesharing")
