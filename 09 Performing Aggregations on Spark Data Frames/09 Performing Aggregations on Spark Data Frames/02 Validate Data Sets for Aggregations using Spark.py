# Databricks notebook source
# MAGIC %fs ls /public/retail_db_json

# COMMAND ----------

import pandas as pd
df=spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/dgadiraju/retail_db/master/categories/part-00000"))

# COMMAND ----------

df.show()

# COMMAND ----------

orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders=spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/dgadiraju/retail_db/master/orders/part-00000"),schema=orders_schema)

# COMMAND ----------


orders.show()

# COMMAND ----------

orders_schema='''
order_customer_id INT,
order_date STRING,
order_id INT,
order_status STRING
'''

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items.printSchema()

# COMMAND ----------

orders_item_schema='''
 order_item_id long, 
 order_item_order_id long, 
 order_item_product_id long,
 order_item_product_price double, 
 order_item_quantity long, 
 order_item_subtotal double 
'''


# COMMAND ----------

orders_item=spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/dgadiraju/retail_db/master/order_items/part-00000"),schema=orders_item_schema)

# COMMAND ----------

orders_item.show()

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

help(count)

# COMMAND ----------

orders.select(count('*')).show()

# COMMAND ----------

orders.count()

# COMMAND ----------

orders.groupBy().min().show()

# COMMAND ----------

orders_item.groupBy().min().show()

# COMMAND ----------

order_items_grouped = orders_item.groupBy()

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------

order_items_grouped.sum()

# COMMAND ----------

order_items_grouped = orders_item. \
    groupBy('order_item_order_id')

# COMMAND ----------

orders_item.show()

# COMMAND ----------

order_items_grouped.count().show()

# COMMAND ----------

order_items_grouped.count().withColumnRenamed('count','order_count').show()

# COMMAND ----------

order_items_grouped.sum().show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.groupBy('order_date').sum().show()

# COMMAND ----------

order_items_grouped = orders_item. \
    select('order_item_order_id', 'order_item_quantity', 'order_item_subtotal'). \
    groupBy('order_item_order_id')

# COMMAND ----------

order_items_grouped

# COMMAND ----------

order_items_grouped.sum().show()

# COMMAND ----------

order_items_grouped.sum('order_item_quantity','order_item_subtotal').show()

# COMMAND ----------

order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    toDF('order_item_order_id', 'order_quantity', 'order_revenue'). \
    show()

# COMMAND ----------

order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    toDF('order_item_order_id', 'order_quantity', 'order_revenue'). \
    withColumn('order_revenue', round('order_revenue', 2)). \
    show()

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

orders_item.groupBy('order_item_order_id').sum().show()

# COMMAND ----------

orders_item.groupBy("order_item_order_id").sum('order_item_quantity', 'order_item_subtotal').show()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,round

# COMMAND ----------

orders_item.show()

# COMMAND ----------

orders_item.groupBy('order_item_order_id').agg(sum('order_item_quantity'), avg('order_item_subtotal')).toDF('order_id','order_quantity','order_subtotal').withColumn('order_subtotal',round('order_subtotal',2)).show()

# COMMAND ----------

sum('order_item_quantityr')

# COMMAND ----------

orders_item.groupBy('order_item_order_id').agg(sum('order_item_quantity').alias('order_id'),round(sum('order_item_subtotal')).alias('order_revenue')).show()

# COMMAND ----------

orders_item.groupBy('order_item_order_id'). \
    agg({'order_item_quantity': 'sum', 'order_item_quantity': 'avg'}). \
    toDF( 'order_revenue', 'order_quantity'). \
    withColumn('order_revenue', round('order_revenue', 2)). \
    show()

# COMMAND ----------

orders_item.filter('order_item_order_id = 2').show()

# COMMAND ----------

orders_item.filter('order_item_order_id = 2').agg(sum('order_item_product_id')).show()

# COMMAND ----------

orders_item.show()

# COMMAND ----------

orders_item.filter('order_item_order_id = 5').agg(sum('order_item_product_id')).show()

# COMMAND ----------

