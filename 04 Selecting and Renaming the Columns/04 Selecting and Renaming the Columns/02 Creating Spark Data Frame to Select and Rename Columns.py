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

import pandas as pd

# COMMAND ----------

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)

# COMMAND ----------

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select("*").show()

# COMMAND ----------

users_df.select('first_name','last_name','email').show()

# COMMAND ----------

users_df.select(['first_name','last_name','email']).show()

# COMMAND ----------

users_df.alias('u').select("u.*").show()

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.selectExpr('last_name+first_name').show()

# COMMAND ----------

users_df.selectExpr('first_name','last_name',"concat(first_name,', ',last_name) as full_name").show()

# COMMAND ----------

users_df.selectExpr('(id*2)as id_2').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT id, first_name, last_name,
        concat(first_name, ', ', last_name) AS full_name
    FROM users
"""). \
    show()

# COMMAND ----------

col('id')

# COMMAND ----------

users_df['id']

# COMMAND ----------

users_df.select('id', col('first_name'), 'last_name').show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

##Concat first_name and last_name. Provide an alias to the derived result as full_name

# COMMAND ----------



# COMMAND ----------

users_df.select('id', 'first_name', 'last_name',concat('first_name', lit(', '), 'last_name').alias('full_name')).show()

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name',concat('first_name', lit(', '), 'last_name').alias('full_name')).show()

# COMMAND ----------

from pyspark.sql.functions import concat, lit,col

# COMMAND ----------

help(users_df.withColumn)

# COMMAND ----------

users_df.select('id','first_name','last_name'). \
withColumn('full_name',concat('first_name', lit(', '), 'last_name')).\
show()

# COMMAND ----------

users_df.select('id').\
withColumn('id2',col('id')*2).\
show()

# COMMAND ----------

users_df.\
withColumn('id2',col('id')*2).\
show()                                      ### adding new columns from existing columns 

# COMMAND ----------

users_df.withColumn("id",col("id")*100).show()   ## tranforming existing column

# COMMAND ----------

users_df.withColumn("user_last_name",col("last_name")).show()

# COMMAND ----------

help(users_df.withColumnRenamed)

# COMMAND ----------

users_df.withColumnRenamed("first_name","user_first_name").show()

# COMMAND ----------

users_df.withColumnRenamed("first_name","user_first_name"). \
        withColumnRenamed("email","user_email"). \
        show()

# COMMAND ----------

user_id=col("id")

# COMMAND ----------

help(user_id.alias)

# COMMAND ----------

users_df. \
       select(
            col("id").alias("user_id"),
            col("last_name").alias("user_last_name"),
            col("first_name").alias("user_last_name")).show()

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------


required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

users_df.select(required_columns).show()

# COMMAND ----------

users_df.select(required_columns). \
        toDF(*target_column_names).show()

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

def myDF(*cols):
    print(type(cols))
    print(cols)


# COMMAND ----------

myDF(*['f1', 'f2'])

# COMMAND ----------

