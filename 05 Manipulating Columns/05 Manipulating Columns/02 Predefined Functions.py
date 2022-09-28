# Databricks notebook source
# MAGIC %md
# MAGIC ## Pre-defined Functions
# MAGIC 
# MAGIC We typically process data in the columns using functions in `pyspark.sql.functions`. Let us understand details about these functions in detail as part of this module.

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us recap about Functions or APIs to process Data Frames.
# MAGIC  * Projection - `select` or `withColumn` or `drop` or `selectExpr`
# MAGIC  * Filtering - `filter` or `where`
# MAGIC  * Grouping data by key and perform aggregations - `groupBy`
# MAGIC  * Sorting data - `sort` or `orderBy` 
# MAGIC * We can pass column names or literals or expressions to all the Data Frame APIs.
# MAGIC * Expressions include arithmetic operations, transformations using functions from `pyspark.sql.functions`.
# MAGIC * There are approximately 300 functions under `pyspark.sql.functions`.
# MAGIC * We will talk about some of the important functions used for String Manipulation, Date Manipulation etc.

# COMMAND ----------

# MAGIC %md
# MAGIC * Here are some of the examples of using functions to take care of required transformations.

# COMMAND ----------

# Reading data

orders = spark.read.csv(
    '/public/retail_db/orders',
    schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

# Importing functions

from pyspark.sql.functions import date_format

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# Function as part of projections

orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month', date_format('order_date', 'yyyyMM')).show()

# COMMAND ----------

# Function as part of where or filter

orders. \
    filter(date_format('order_date', 'yyyyMM') == 201401). \
    show()

# COMMAND ----------

# Function as part of groupBy

orders. \
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
    count(). \
    show()

# COMMAND ----------

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

import pandas as pd

# COMMAND ----------

df1=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

df1.show()

# COMMAND ----------

help(filter)

# COMMAND ----------

help(df1.groupBy)

# COMMAND ----------

def new():
    

df1.filter(col('last_name',df1.last_name=='Brewitt')).show()

# COMMAND ----------

orders. \
    filter(date_format('order_date', 'yyyyMM') == 201401). \
    show()

orders. \
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
    count(). \
    show()

# COMMAND ----------

