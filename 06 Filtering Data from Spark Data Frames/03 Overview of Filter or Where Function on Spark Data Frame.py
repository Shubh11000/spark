# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

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
        "gender": "male",
        "current_city": "Dallas",
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
        "gender": "male",
        "current_city": "Houston",
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
        "gender": "female",
        "current_city": "",
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
        "gender": "male",
        "current_city": "San Fransisco",
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
        "gender": "female",
        "current_city": None,
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
users_df=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.filter)

# COMMAND ----------

users_df.where?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * `where` and `filter` are synonyms
# MAGIC * We can pass conditions either by using SQL Style or Non SQL Style.
# MAGIC * For Non SQL Style we can pass columns using `col` function on column name as string or using the notation of`df['column_name']`

# COMMAND ----------

from pyspark.sql.functions import col,isnan

# COMMAND ----------

users_df.filter(col('id')==1).show()

# COMMAND ----------

users_df.filter(col('id') == 1).show()

# COMMAND ----------

users_df.where(col('id') == 1).show()

# COMMAND ----------

users_df.filter(users_df['id'] == 1).show()

# COMMAND ----------

users_df.where(users_df['id'] == 1).show()

# COMMAND ----------

# 'id == 1' also works
users_df.filter('id = 1').show()

# COMMAND ----------

users_df.filter('id=1 or id=2').show()

# COMMAND ----------

users_df.where('id = 1').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM users
    WHERE id = 1
"""). \
    show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.filter(col('is_customer')==True).show()

# COMMAND ----------

users_df.filter("is_customer=='true'").show()

# COMMAND ----------

users_df.filter(col('amount_paid')=="1000.55").show()

# COMMAND ----------

users_df.select(isnan('amount_paid')).show()

# COMMAND ----------

help(isnan)

# COMMAND ----------

users_df.filter(isnan('amount_paid')==True).show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql('''
    SELECT * FROM users
    WHERE is_customer = "true"
'''). \
    show()

# COMMAND ----------

users_df.filter('isnan(amount_paid)!=True').show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.filter(col('current_city')!="").show()

# COMMAND ----------

users_df.filter((col('current_city')=="")|(col('current_city')=="Dallas")).show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.filter(col('last_updated_ts').between('2021-02-10 01:15:00','2021-03-15 15:16:55')).show()

# COMMAND ----------

users_df.filter("amount_paid BETWEEN 850 AND 900").show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.filter(col('current_city').isNotNull()).show()

# COMMAND ----------

users_df.filter(col('current_city').isNull()).show()

# COMMAND ----------

users_df.filter("current_city IS NULL").show()

# COMMAND ----------

users_df.filter("current_city IS NOT NULL").show()

# COMMAND ----------

users_df.filter((col('last_name')=="Penney")|(col('last_name')=="Maddocks")).show()

# COMMAND ----------

users_df.filter(col('last_name').isin("Penney","Maddocks")).show()

# COMMAND ----------

users_df.filter("last_name='Penney' or last_name='Maddocks'").show()

# COMMAND ----------

users_df.select(col('current_city').isNull()).show()

# COMMAND ----------

users_df.filter((col('current_city')=="")|(col('current_city').isNull())).show()

# COMMAND ----------

