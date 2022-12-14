# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

from pyspark.sql.functions import sort_array

# COMMAND ----------

users_df.sort(col("courses").desc()).show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)

# COMMAND ----------

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

from pyspark.sql.functions import col,size,desc

# COMMAND ----------

users_df.sort("first_name").show()

# COMMAND ----------

users_df.sort(col("first_name").asc()).show()

# COMMAND ----------


users_df.select('*').sort(size(col("courses"))).withColumn('no_of_size',size('courses')).show()

# COMMAND ----------

users_df.sort(col("first_name").desc()).show()

# COMMAND ----------

users_df.sort(users_df.first_name.desc()).show()

# COMMAND ----------

users_df.sort('first_name',ascending=False).show()

# COMMAND ----------

users_df.sort("first_name",ascending=False).show()

# COMMAND ----------

users_df.sort(col("first_name").desc()).show()

# COMMAND ----------

users_df.sort(desc('first_name')).show()

# COMMAND ----------

users_df.orderBy('customer_from').show()

# COMMAND ----------

users_df.orderBy(col('customer_from').asc_nulls_last()).show()

# COMMAND ----------

users_df.orderBy(col('customer_from').desc()).show()

# COMMAND ----------

users_df.orderBy(col('customer_from').desc_nulls_last()).show()

# COMMAND ----------

