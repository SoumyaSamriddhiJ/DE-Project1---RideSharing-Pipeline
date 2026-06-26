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

spark = SparkSession.builder.appName("Gold_layer").getOrCreate()
gold_df = spark.read.format("delta").table("ride_catalogue.default.silver_ridesharing")

# COMMAND ----------

gold_df.createOrReplaceTempView("gold_temp")

gold_df = spark.sql("""
    SELECT *,
    coalesce(
        try_to_timestamp(pickup_datetime, 'MM-dd-yyyy'),
        try_to_timestamp(pickup_datetime, 'yyyy/MM/dd HH:mm'),
        try_to_timestamp(pickup_datetime, 'MMMM dd, yyyy'),
        try_to_timestamp(pickup_datetime, 'MM/dd/yyyy'),
        try_to_timestamp(pickup_datetime, 'yyyy-MM-dd'),
        try_to_timestamp(pickup_datetime, 'dd/MM/yyyy'),
        try_to_timestamp(pickup_datetime, 'dd-MMM-yyyy'),
        try_to_timestamp(pickup_datetime, 'dd.MM.yyyy')
    ) as pickup_datetime_clean,
    coalesce( 
        try_to_timestamp(dropoff_datetime, 'MM-dd-yyyy'),
        try_to_timestamp(dropoff_datetime, 'yyyy/MM/dd HH:mm'),
        try_to_timestamp(dropoff_datetime, 'MMMM dd, yyyy'),
        try_to_timestamp(dropoff_datetime, 'MM/dd/yyyy'),
        try_to_timestamp(dropoff_datetime, 'yyyy-MM-dd'),
        try_to_timestamp(dropoff_datetime, 'dd/MM/yyyy'),
        try_to_timestamp(dropoff_datetime, 'dd-MMM-yyyy'),
        try_to_timestamp(dropoff_datetime, 'dd.MM.yyyy') 
    ) as dropoff_datetime_clean
    FROM gold_temp
""")

# COMMAND ----------

gold_df = gold_df.drop("pickup_datetime").drop("dropoff_datetime")

# COMMAND ----------

gold_df = gold_df.withColumnRenamed("pickup_datetime_clean", "pickup_datetime") \
                 .withColumnRenamed("dropoff_datetime_clean", "dropoff_datetime")

# COMMAND ----------

gold_df = gold_df.withColumn("pickup_datetime", date_format(col("pickup_datetime"), "yyyy-MM-dd HH:mm")) \
                 .withColumn("dropoff_datetime", date_format(col("dropoff_datetime"), "yyyy-MM-dd HH:mm"))

# COMMAND ----------

gold_df = gold_df.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
                 .withColumn("dropoff_hour", hour(col("dropoff_datetime")))

# COMMAND ----------

gold_df = gold_df.drop("dropoff_hour")

# COMMAND ----------

gold_df = gold_df.fillna({"pickup_hour":-1,"pickup_datetime":"0000-00-00 00:00","dropoff_datetime":"0000-00-00 00:00"})

# COMMAND ----------

driver_revenue_summary = gold_df.groupBy("driver_id","driver_name")\
                .agg(round(sum("total_fare"),2).alias("total_revenue"))\
                .orderBy("total_revenue", ascending = False)
#driver_revenue_summary.display()


# COMMAND ----------

driver_rating_summary = gold_df.groupby("driver_id","driver_name")\
                            .agg(avg("driver_rating").alias("avg_rating"))\
                            .orderBy("avg_rating",ascending= False)

# driver_rating_summary.display()


# COMMAND ----------

vehicle_revenue_summary = gold_df.groupBy("vehicle_type")\
                      .agg(round(sum("total_fare"),2).alias("sum_veh_type"))\
                      .orderBy("sum_veh_type",ascending = False)

# vehicle_revenue_summary.display()



# COMMAND ----------

rider_trip_summary = gold_df.groupBy("rider_id","rider_name")\
            .agg(count("trip_id").alias("total_rides"))\
            .orderBy("total_rides",ascending = False)

# rider_trip_summary.display()



# COMMAND ----------

payment_revenue_summary = gold_df.groupBy("payment_type")\
                  .agg(sum("total_fare").alias("rev_pay_type"))\
                  .orderBy("rev_pay_type",ascending = False)

# payment_revenue_summary.display()

# COMMAND ----------

peak_hours_summary = gold_df.filter(col("pickup_hour")!= -1)\
        .groupBy("pickup_hour")\
        .agg(count("trip_id").alias("peak_hour"))\
        .orderBy("peak_hour",ascending = False)

# peak_hours_summary.display()

# COMMAND ----------

cancellation_summary = gold_df.groupBy("cancellation_reason")\
                 .agg(count("trip_id").alias("cancelled_rides"))\
                 .orderBy("cancelled_rides",ascending=False)

# cancellation_summary.display()

# COMMAND ----------

top_pickup_summary = gold_df.groupBy("pickup_location")\
                    .agg(count("trip_id").alias("visited_places"))\
                    .orderBy("visited_places",ascending=False)

# top_pickup_summary.display()

# COMMAND ----------

top_dropoff_summary = gold_df.groupBy("dropoff_location")\
                    .agg(count("trip_id").alias("dropoff_places"))\
                    .orderBy("dropoff_places",ascending=False)

# top_dropoff_summary.display()

# COMMAND ----------

platform_summary = gold_df.groupBy("os_platform")\
        .agg(count("trip_id").alias("os_count"))\
        .orderBy("os_count",ascending=False)

# platform_summary.display()

# COMMAND ----------

surge_zone_summary = gold_df.groupBy("surge_zone")\
        .agg(count("trip_id").alias("surge_zone_count"))\
        .orderBy("surge_zone_count",ascending=False)

# surge_zone_summary.display()


# COMMAND ----------

gold_df.display()

# COMMAND ----------

gold_df.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.gold_ridesharing")

driver_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.driver_revenue_summary")
rider_trip_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.rider_trip_summary")
driver_rating_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.driver_rating_summary")
vehicle_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.vehicle_revenue_summary")
payment_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.payment_revenue_summary")
peak_hours_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.peak_hours_summary")
cancellation_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.cancellation_summary")
top_pickup_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.top_pickup_summary")
top_dropoff_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.top_dropoff_summary")
platform_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.platform_summary")
surge_zone_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.surge_zone_summary")