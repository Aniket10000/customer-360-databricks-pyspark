# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/orders/orders.csv", header=True, inferSchema=True)
df_bronze_orders = df.withColumn("ingestion_timestamp", current_timestamp())
# writing the data to the delta
df_bronze_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_q.ecommerce_bronze.orders")
order_bronze= spark.table("retail_q.ecommerce_bronze.orders")

# order_bronze.show()
# drop the duplicate and amount > 0
filter_df = order_bronze.dropDuplicates().filter(col("total_amount")>0)
orders_df = filter_df.filter(col("status").isin('COMPLETED','CREATED','SHIPPED','CANCELLED'))




# COMMAND ----------

# customer.csv ingestion and loading

customer_df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/customers/customers.csv", header=True, inferSchema = True)
customer_df = customer_df.withColumn("ingestion_timestamp", current_timestamp())
customer_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_q.ecommerce_bronze.customers")



# COMMAND ----------

# order_items ingestion

orders_df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/order_items/order_items.csv", header=True, inferSchema = True)
orders_df = orders_df.withColumn("ingestion_timestamp", current_timestamp())
orders_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_q.ecommerce_bronze.order_items")



# COMMAND ----------

# payment ingestion

payment_df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/payments/payments.csv", header=True, inferSchema = True)
payment_df = payment_df.withColumn("ingestion_timestamp", current_timestamp())
payment_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_q.ecommerce_bronze.payments")


# COMMAND ----------

# product ingestion

product_df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/products/products.csv", header=True, inferSchema = True)
product_df = product_df.withColumn("ingestion_timestamp", current_timestamp())
product_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_q.ecommerce_bronze.products")


# COMMAND ----------

spark.sql("""
SHOW TABLES IN retail_q.ecommerce_bronze
""").show()

for table in ["customers", "products", "orders", "order_items", "payments"]:
    count = spark.table(
        f"retail_q.ecommerce_bronze.{table}"
    ).count()
    print(table, count)