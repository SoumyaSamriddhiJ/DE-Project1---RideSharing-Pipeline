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

spark = SparkSession.builder.appName("Validation").getOrCreate()
val_df = spark.read.format("delta").table("ride_catalogue.default.silver_ridesharing")

# COMMAND ----------

row_count = val_df.count()
assert row_count > 0, "Validation Failed: No rows in silver table!"
print(f"Row count check passed: {row_count} rows")

# COMMAND ----------

duplicate_count = val_df.count() - val_df.dropDuplicates(["trip_id"]).count()
assert duplicate_count == 0, f"Validation Failed: {duplicate_count} duplicate trip_ids found!"
print(f"Duplicate check passed: No duplicates found")

# COMMAND ----------

negative_fares = val_df.filter(col("total_fare") < 0).count()
assert negative_fares == 0, f"Validation Failed: {negative_fares} negative fares found!"
print(f"Total fare check passed: No negative fares")

# COMMAND ----------

expected_cols = 33
actual_cols = len(val_df.columns)
assert actual_cols == expected_cols, f"Validation Failed: Expected {expected_cols} columns but got {actual_cols}!"
print(f"Column count check passed: {actual_cols} columns")

# COMMAND ----------

expected_schema = {
    "trip_id": "string",
    "rider_id": "string",
    "driver_id": "string",
    "trip_duration_mins": "int",
    "pickup_lat": "double",
    "pickup_lng": "double",
    "dropoff_lat": "double",
    "dropoff_lng": "double",
    "distance_km": "double",
    "base_fare": "double",
    "surge_multiplier": "double",
    "tip": "double",
    "total_fare": "double",
    "driver_rating": "double",
    "rider_rating": "double",
    "promo_discount": "double"
}

actual_schema = dict(val_df.dtypes)

for col_name, expected_type in expected_schema.items():
    assert actual_schema[col_name] == expected_type, f"Validation Failed: {col_name} should be {expected_type} but got {actual_schema[col_name]}!"

print("Data type check passed: All columns have correct types")

# COMMAND ----------

critical_cols = ["trip_id", "rider_id", "driver_id"]
for col_name in critical_cols:
    null_count = val_df.filter(col(col_name).isNull()).count()
    assert null_count == 0, f"Validation Failed: {null_count} nulls found in {col_name}!"
print("Null check passed: No nulls in critical columns")

# COMMAND ----------

invalid_ratings = val_df.filter(
    (col("driver_rating") < 1) | (col("driver_rating") > 5) |
    (col("rider_rating") < 1) | (col("rider_rating") > 5)
).count()
assert invalid_ratings == 0, f"Validation Failed: {invalid_ratings} invalid ratings found!"
print("Rating range check passed: All ratings between 1-5")

# COMMAND ----------

invalid_values = val_df.filter(
    (col("distance_km") <= 0) | (col("trip_duration_mins") <= 0)
).count()
assert invalid_values == 0, f"Validation Failed: {invalid_values} invalid values found!"
print("Positive values check passed: All values are positive")