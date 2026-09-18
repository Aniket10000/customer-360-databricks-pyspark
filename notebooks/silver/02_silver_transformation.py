# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

orders_bronze = spark.table("retail_q.ecommerce_bronze.orders")

customer_bronze = spark.table("retail_q.ecommerce_bronze.customers")

orders_clean = orders_bronze.dropDuplicates().filter(col("total_amount")>0)
orders_clean = orders_clean.filter(col("status").isin('COMPLETED','CREATED','SHIPPED','CANCELLED'))
orders_clean = orders_clean.filter((col("customer_id").isNotNull()) & (col("order_id").isNotNull()) & (col("order_date").isNotNull()))

invalid_customers_df =  orders_clean.join(customer_bronze, customer_bronze.customer_id==orders_clean.customer_id, "leftanti")

rejected_customer_df = invalid_customers_df.withColumn("rejection_timestamp", current_timestamp())\
                    .withColumn("rejection_reason", lit("Customer not found"))

valid_orders_df = orders_clean.join(customer_bronze,customer_bronze.customer_id==orders_clean.customer_id,"left_semi" )

silver_orders_df = valid_orders_df.select("order_id","customer_id","order_date","status","total_amount","ingestion_timestamp")

silver_orders_df.write.format("delta").mode("overwrite").saveAsTable("retail_q.retail_silver.orders")
rejected_customer_df.write.format("delta").mode("overwrite").saveAsTable("retail_q.retail_silver.orders_quarantine")

# COMMAND ----------

spark.sql("show tables in retail_q.retail_silver").show()

spark.sql(""" select count(*) from retail_q.retail_silver.orders """).show()
spark.sql(""" select * from retail_q.retail_silver.orders_quarantine """).show()

# COMMAND ----------

order_items_bronze_df = spark.table("retail_q.ecommerce_bronze.order_items")
invalid_quantity_df = order_items_bronze_df.filter(col("quantity")<=0)

invalid_quantity_df = invalid_quantity_df.withColumn("rejection_timestamp", current_timestamp())\
                    .withColumn("rejection_reason", lit("Invalid order quantity"))
orders_clean_df = order_items_bronze_df.filter(col("quantity")>0)
orders_silver_df = spark.table("retail_q.retail_silver.orders")

invalid_records =orders_clean_df.join(orders_silver_df, orders_clean_df.order_id ==orders_silver_df.order_id, "left_anti")
invalid_records = invalid_records.withColumn("rejection_timestamp", current_timestamp())\
                    .withColumn("rejection_reason", lit("Order not found in silver"))

valid_records =orders_clean_df.join(orders_silver_df, orders_clean_df.order_id ==orders_silver_df.order_id, "left_semi")

rejected_df = invalid_quantity_df.unionByName(invalid_records)

rejected_df.select("order_id",
    "product_id",
    "quantity",
    "rejection_reason").show()
valid_records.write.format("delta").mode("overwrite").saveAsTable("retail_q.retail_silver.order_items")




# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

spark.sql("Select * from  retail_q.retail_silver.order_items").show()

# COMMAND ----------

products_bronze_df = spark.table(
    "retail_q.ecommerce_bronze.products"
)
# products_bronze_df.show()

order_items_silver_df  = spark.table("retail_q.retail_silver.order_items")
# valid_order_df.show()

invalid_products_df = order_items_silver_df .join(products_bronze_df,products_bronze_df.product_id== order_items_silver_df .product_id,"left_anti")

invalid_products_df = invalid_products_df.withColumn("rejection_timestamp", current_timestamp()).withColumn("rejection_reason", lit("product not found"))

final_rejected_df = invalid_products_df.unionByName(invalid_records, allowMissingColumns=True).unionByName(invalid_quantity_df, allowMissingColumns=True)

final_rejected_df = final_rejected_df.select("order_id",
    "product_id",
    "quantity",
    "rejection_reason")

final_rejected_df.groupBy("rejection_reason").count().show()

final_rejected_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_q.retail_silver.order_items_quarantine")

# COMMAND ----------

print("Bronze:", spark.table(
    "retail_q.ecommerce_bronze.order_items"
).count())

print("Silver:", spark.table(
    "retail_q.retail_silver.order_items"
).count())

print("Quarantine:", spark.table(
    "retail_q.retail_silver.order_items_quarantine"
).count())


# COMMAND ----------

mySchema = StructType([
    StructField("customer_id", IntegerType(),False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", DateType(), True)]
)
df = spark.read.csv("/Volumes/retail_q/volumes/ecommerce_raw/raw/customers/customers.csv" , header=True, schema = mySchema)

df.printSchema()
df.show()



# COMMAND ----------

customers_bronze_df = spark.table(
    "retail_q.ecommerce_bronze.customers"
)
# customers_bronze_df.show(truncate=False)

duplicate_customer_df = customers_bronze_df.groupBy(  "customer_id",
    "name",
    "email",
    "city",
    "signup_date").count().filter(col("count")>1)
duplicate_customer_df.show()

customer_clean_df = customers_bronze_df.withColumn("name", trim(col("name")))\
                    .withColumn("email", lower(col("email")))\
                    .withColumn("city", trim(col("city"))).dropDuplicates()

# customer_id IS NULL
# OR name IS NULL
# OR email IS NULL
invalid_customer_df = customer_clean_df.filter((col("customer_id").isNull()) | (col("name").isNull()) | (col("email").isNull())) 
invalid_customer_df = invalid_customer_df.withColumn(
    "rejection_timestamp", current_timestamp()
).withColumn(
    "rejection_reason",
    when(col("customer_id").isNull(), lit("customer_id is null"))
    .when(col("name").isNull(), lit("name is null"))
    .when(col("email").isNull(), lit("email is null"))
    .otherwise(
        lit("unknown")
    ),  # Optional: defines default value if none match
)

conflict_ids_df = customer_clean_df \
    .groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1)
customer_conflict_df = customer_clean_df.join(
    conflict_ids_df.select("customer_id"),
    on="customer_id",
    how="inner"
)
customer_conflict_df = customer_conflict_df.withColumn("rejection_reason", lit("customer conflict")).withColumn("rejection_timestamp", current_timestamp())


customer_df_silver = customer_clean_df.alias("clean") \
    .join(invalid_customer_df.alias("invalid"), col("clean.customer_id") == col("invalid.customer_id"), "left_anti") \
    .join(customer_conflict_df.alias("conflict"), col("clean.customer_id") == col("conflict.customer_id"), "left_anti")
customer_df_silver.show()
                                                                 

# COMMAND ----------

customer_df_silver.count()

# COMMAND ----------

invalid_customer_df.count()
customer_conflict_df.count()

# COMMAND ----------

products_bronze_df = spark.table(
    "retail_q.ecommerce_bronze.products"
)
products_bronze_df.show()

duplicate_products_df = products_bronze_df \
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "price"
    ) \
    .count() \
    .filter(col("count") > 1)


products_clean_df = products_bronze_df \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", trim(col("category"))) \
    .withColumn("price", col("price")) \
    .dropDuplicates() 
invalid_column_check_df = products_clean_df.filter(
    col("product_id").isNull() |
    col("product_name").isNull() |
    col("category").isNull() |
    col("price").isNull()
)

invalid_column_check_df = invalid_column_check_df.withColumn(
    "rejection_timestamp",
    current_timestamp()
).withColumn(
    "rejection_reason",
    when(col("product_id").isNull(), lit("PRODUCT_ID_NULL"))
    .when(col("product_name").isNull(), lit("PRODUCT_NAME_NULL"))
    .when(col("category").isNull(), lit("CATEGORY_NULL"))
    .when(col("price").isNull(), lit("PRICE_NULL"))
    .when(col("price") <= 0, "INVALID_PRICE")
)

valid_candidate_df = products_clean_df.filter(
    col("product_id").isNotNull() &
    col("product_name").isNotNull() &
    col("category").isNotNull() &
    (col("price") > 0)
)

conflict_ids_df = valid_candidate_df \
    .groupBy("product_id") \
    .count() \
    .filter(col("count") > 1)

product_conflict_df = products_clean_df.join(
    conflict_ids_df.select("product_id"),
    on="product_id",
    how="inner"
)
product_conflict_df = product_conflict_df \
    .withColumn("rejection_timestamp", current_timestamp()) \
    .withColumn("rejection_reason", lit("PRODUCT_DATA_CONFLICT"))

product_silver_df = valid_candidate_df.join(
    conflict_ids_df.select("product_id"),
    on="product_id",
    how="left_anti"
)
# product_silver_df.show()


product_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_q.retail_silver.products"
)
product_quarantine_df = invalid_column_check_df.select(
    "product_id",
    "product_name",
    "category",
    "price",
    "rejection_timestamp",
    "rejection_reason"
).unionByName(
    product_conflict_df.select(
        "product_id",
        "product_name",
        "category",
        "price",
        "rejection_timestamp",
        "rejection_reason"
    )
)
product_quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_q.retail_silver.products_quarantine"
    )






# COMMAND ----------

spark.table("retail_q.retail_silver.products").show()
spark.table("retail_q.retail_silver.products_quarantine").show()

# COMMAND ----------

payments_bronze_df = spark.table(
    "retail_q.ecommerce_bronze.payments"
)

orders_silver_df = spark.table(
    "retail_q.retail_silver.orders"
)
payments_bronze_df.show()
# orders_silver_df.show()

missing_payment_df = payments_bronze_df.filter(col("payment_id").isNull() | col("order_id").isNull() | col("payment_method").isNull() | col("payment_date").isNull() | col("amount").isNull() | (~col("payment_status").isin(['SUCCESS','PENDING','REFUNDED','FAILED'])))

missing_payment_df = missing_payment_df.withColumn(
    "rejection_timestamp",
    current_timestamp())\
    .withColumn("rejection_reason" , when(col("payment_id").isNull(), lit("PAYMENT_ID_NULL"))\
    .when(col("order_id").isNull(), lit("ORDER_ID_NULL"))\
    .when(col("payment_method").isNull(), lit("PAYMENT_METHOD_NULL"))\
    .when((~col("payment_method").isin('UPI','CARD')), lit("INVALID PAYMENT_METHOD"))\
    .when(col("payment_date").isNull(), lit("PAYMENT_DATE_NULL"))\
    .when(col("amount").isNull(), lit("AMOUNT_NULL"))\
    .when(col("amount")<=0, lit("AMOUNT_INVALID"))\
    .when(~col("payment_status").isin(['SUCCESS','PENDING','REFUNDED','FAILED']), lit("PAYMENT_STATUS_INVALID"))\
    )
missing_payment_df.show()



valid_payment_candidates_df = payments_bronze_df.filter(
    col("payment_id").isNotNull() &
    col("order_id").isNotNull() &
    col("payment_date").isNotNull() &
    col("payment_method").isin("UPI", "CARD") &
    col("amount").isNotNull() &
    (col("amount") > 0) &
    col("payment_status").isin(
        "SUCCESS", "PENDING", "REFUNDED", "FAILED"
    )
)

invalid_payments_df = valid_payment_candidates_df.join(
    orders_silver_df,
    valid_payment_candidates_df.order_id == orders_silver_df.order_id,
    "left_anti"
)
invalid_payments_df = invalid_payments_df.withColumn("rejection_timestamp",
                                                     current_timestamp()) \
    .withColumn("rejection_reason", lit("ORDER_ID_NOT_FOUND"))

payment_silver_df = valid_payment_candidates_df.join(
    orders_silver_df,
    valid_payment_candidates_df.order_id == orders_silver_df.order_id,
    "left_semi"
)
payment_silver_df = payment_silver_df.select(
    "payment_id",
    "order_id",
    "payment_date",
    "payment_method",
    "amount",
    "payment_status"
)

payment_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_q.retail_silver.payments"
    )

payment_quarantine_df = invalid_payments_df.select(
    "payment_id",
    "order_id",
    "payment_date",
    "payment_method",
    "amount",
    "payment_status",
    "rejection_timestamp",
    "rejection_reason"
).unionByName(
    missing_payment_df.select(
        "payment_id",
        "order_id",
        "payment_date",
        "payment_method",
        "amount",
        "payment_status",
        "rejection_timestamp",
        "rejection_reason"
    )
)
payment_quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_q.retail_silver.payments_quarantine"
    )

# COMMAND ----------

spark.table("retail_q.retail_silver.payments").count()
spark.table(
    "retail_q.retail_silver.payments_quarantine"
).count()