# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

silver_orders_df = spark.table(
    "retail_q.customer360_silver.orders"
)

# silver_orders_df.count()

order_metric_df = silver_orders_df.groupBy("customer_id").agg( count("order_id").alias("total_orders"),\
                   sum(when (col("order_status")=='COMPLETED',1).otherwise(0)).alias("completed_orders"),\
                   sum(when(col("order_status")=='CANCELLED',1).otherwise(0)).alias("cancelled_orders"),\
                   sum(when(col("order_status")=='COMPLETED', col("order_amount")).otherwise(0)).alias("total_spend"),\
                   avg(when(col("order_status")=='COMPLETED', col("order_amount")).otherwise(0)).alias("avg_spend"),\
                   min("order_date").alias("first_order_date"),\
                   max("order_date").alias("Last_order_date")
                   )   

order_metric_df = order_metric_df.orderBy('customer_id')
                    
    


# COMMAND ----------

# payment metrics

silver_payment_df = spark.table("retail_q.customer360_silver.payments")

display(silver_payment_df)

payments_metric_df = silver_payment_df.groupBy("customer_id").agg(\
    count("payment_id").alias("total_payments"),\
    sum(when(col("payment_status")=='SUCCESS',1).otherwise(0)).alias("successfull_payments"),\
    sum(when(col("payment_status")=='FAILED',1).otherwise(0)).alias("failed_payments"),\
    sum(when(col("payment_status")=='SUCCESS',col("amount")).otherwise(0)).alias("total_spend"),\
        
)
payments_metric_df.orderBy("customer_id").show()

payment_method_count_df = silver_payment_df.groupBy("customer_id", "payment_method").agg(count("*").alias("payment_usg_count"))

payment_method_count_df.orderBy("customer_id").show()

windowSpec = Window.partitionBy("customer_id").orderBy(col("payment_usg_count").desc(), col("payment_method"))

preferred_payment_method = payment_method_count_df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn")==1).select("customer_id", col("payment_method").alias("preferred_payment_method") )

preferred_payment_method.orderBy("customer_id").show()

final_payment_metrics = payments_metric_df.join(preferred_payment_method,preferred_payment_method.customer_id== payments_metric_df.customer_id, "left")
final_payment_metrics.show()




# COMMAND ----------

silver_web_event_df = spark.table("retail_q.customer360_silver.web_events")

silver_web_event_df.orderBy("customer_id").show()

web_login_metrics = silver_web_event_df.groupBy("customer_id").agg(\

    sum(when(col("event_type")=="LOGIN",1).otherwise(0)).alias("Total_logins"),\
    sum(when(col("event_type")=="CHECKOUT",1).otherwise(0)).alias("checkout_count"),\
    sum(when(col("event_type")=='PRODUCT_VIEW',1).otherwise(0)).alias("product_views"),\
    sum(when(col("event_type")=='ADD_TO_CART',1).otherwise(0)).alias("add_to_cart_count"),\
    max(col("event_timestamp")).alias("last_activity")

)
web_login_metrics.orderBy("customer_id").show()

# COMMAND ----------

silver_support_ticket_df = spark.table("retail_q.customer360_silver.support_tickets")
silver_support_ticket_df.orderBy("customer_id").show()

support_ticket_metrics = silver_support_ticket_df.groupBy("customer_id").agg(count("ticket_id").alias("total_tickets"),\
    sum(when(col("ticket_status")=='OPEN',1).otherwise(0)).alias("total_open_tickets"),\
    avg((col("resolution_time_hours"))).alias("avg_resolution_time")
)

support_ticket_metrics.orderBy("customer_id").show()

# COMMAND ----------

silver_campaign_df = spark.table("retail_q.customer360_silver.campaigns")

# silver_campaign_df.show()


campaign_metrics_df = silver_campaign_df.groupBy("customer_id").agg(count("campaign_id").alias("campaign_received"),\
                                sum(when(col("response")=='CLICKED',1).otherwise(0)).alias("campaigns_clicked"))    
    
# campaign_metrics_df.orderBy("customer_id").show()




# COMMAND ----------

silver_customers_df = spark.table(
    "retail_q.customer360_silver.customers"
)

# Rename duplicate customer_id column and drop it
final_payment_metrics_clean = final_payment_metrics.toDF(
    'customer_id', 'total_payments', 'successfull_payments', 'failed_payments', 
    'total_spend ', 'customer_id_dup', 'Preferred_Payment_Method'
).drop('customer_id_dup')

customer_360_df = silver_customers_df.alias('c')\
    .join(order_metric_df.alias('o'), col("c.customer_id")==col("o.customer_id"), 'left')\
    .join(final_payment_metrics_clean.alias("p"), col("c.customer_id")==col("p.customer_id"), "left")\
    .join(web_login_metrics.alias("w"), col("c.customer_id")==col("w.customer_id"),"left")\
    .join(support_ticket_metrics.alias("s"), col("c.customer_id")==col("s.customer_id"), "left")\
    .join(campaign_metrics_df.alias("cm"), col("c.customer_id")==col("cm.customer_id"), "left")
                   
                   
# print("Customer 360 count:", customer_360_df.count())

customer_360_df.show()
                   







# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

final_customer_360_df = customer_360_df.select(
    
    # Customer Identity
    col("c.customer_id").alias("customer_id"),
    col("c.customer_name").alias("customer_name"),
    col("c.email").alias("email"),
    col("c.city").alias("city"),
    col("c.signup_date").alias("signup_date"),

    # Order Metrics
    coalesce(col("o.total_orders"), lit(0)).alias("o.total_orders"),
    coalesce(col("o.completed_orders"), lit(0)).alias("o.completed_orders"),
    coalesce(col("o.cancelled_orders"), lit(0)).alias("o.cancelled_orders"),
    coalesce(col("o.total_spend"), lit(0)).alias("o.total_spend"),
    coalesce(col("o.avg_spend"), lit(0)).alias("o.avg_spend"),
    col("o.first_order_date").alias("o.first_order_date"),
    col("o.last_order_date").alias("o.last_order_date"),

    # Payment Metrics
    coalesce(col("p.total_payments"), lit(0)).alias("p.total_payments"),
    coalesce(col("p.successfull_payments"), lit(0)).alias("p.successfull_payments"),
    coalesce(col("p.failed_payments"), lit(0)).alias("p.failed_payments"),
    col("p.preferred_payment_method").alias("p.preferred_payment_method"),

    # Web Metrics
    coalesce(col("w.total_logins"), lit(0)).alias("w.total_logins"),
    coalesce(col("w.product_views"), lit(0)).alias("product_views"),
    coalesce(col("w.add_to_cart_count"), lit(0)).alias("add_to_cart_count"),
    coalesce(col("w.checkout_count"), lit(0)).alias("checkout_count"),
    col("w.last_activity").alias("last_activity"),

    # Support Metrics
    coalesce(col("s.total_tickets"), lit(0)).alias("total_tickets"),
    coalesce(col("s.total_open_tickets"), lit(0)).alias("s.total_open_tickets"),
    coalesce(col("s.avg_resolution_time"), lit(0)).alias("avg_resolution_time"),

    # Campaign Metrics
    coalesce(col("cm.campaign_received"), lit(0)).alias("cm.campaign_received"),
    coalesce(col("cm.campaigns_clicked"), lit(0)).alias("cm.campaigns_clicked")
)

final_customer_360_df = final_customer_360_df.withColumn("customer_segment", when(col("`o.total_spend`")>=20000,lit('High'))\
                    .when(col("`o.total_spend`")>=10000,lit('Mid'))\
                    .otherwise(lit('Regular'))
                    
).withColumn("customer_risk", when(col("`p.failed_payments`")>=2,lit('High'))\
                    .when(col("`s.total_open_tickets`")>=1,lit('Mid'))\
                    .otherwise(lit('Regular'))
                    
)

display(
    final_customer_360_df.select(
        "customer_id",
        "`o.total_orders`",
        "`o.total_spend`",
        "`p.total_payments`",
        "`w.total_logins`",
        "total_tickets",
        "`cm.campaign_received`",
        "customer_segment",
        "customer_risk"
    ).orderBy("customer_id")
)
print("final_customer_360_df count ",final_customer_360_df.count())

final_customer_360_df.groupBy("customer_id").agg(count("*").alias("cnt")).filter(col("cnt")>1).show()

# Rename columns to avoid dots for summary statistics
summary_df = final_customer_360_df.select(
    "customer_id",
    col("`o.total_orders`").alias("total_orders"),
    col("`o.total_spend`").alias("total_spend"),
    col("`p.total_payments`").alias("total_payments"),
    col("`w.total_logins`").alias("total_logins"),
    "total_tickets",
    col("`cm.campaign_received`").alias("campaign_received"),
    "customer_segment",
    "customer_risk"
)

display(summary_df.summary())


# COMMAND ----------

null_check = final_customer_360_df.select(
    [
        sum(when(col(f"`{c}`").isNull(), 1).otherwise(0)).alias(c)
        for c in final_customer_360_df.columns
    ]
)

display(null_check)

display(
    final_customer_360_df.filter(
        (col("total_orders") < 0) |
        (col("total_spend") < 0) |
        (col("total_payments") < 0) |
        (col("total_logins") < 0) |
        (col("total_tickets") < 0) |
        (col("campaign_received") < 0)
    )
)

final_customer_360_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_q.retail_gold.customer_360")

# COMMAND ----------

gold_customer_360 = spark.table("retail_q.retail_gold.customer_360")

customer_seg_df = gold_customer_360.groupBy("customer_segment").agg(count("*"))

customer_seg_df.show()

from pyspark.sql.functions import round

total_customers = final_customer_360_df.count()

segment_distribution_df = (
    gold_customer_360
    .groupBy("customer_segment")
    .count()
    .withColumn(
        "customer_percentage",
        round(col("count") / total_customers * 100, 2)
    )
    .orderBy("customer_segment")
)

display(segment_distribution_df)

from pyspark.sql.functions import col, desc

high_value_customers_df = (
    gold_customer_360
    .filter(col("`o.total_spend`") >= 10000)
    .select(
        "customer_id",
        "`o.total_orders`",
        "`o.total_spend`",
        "`p.total_payments`",
        "`w.total_logins`",
        "total_tickets",
        "`cm.campaign_received`",
        "customer_segment",
        "customer_risk"
    )
    .orderBy(desc("`o.total_spend`"))
)

display(high_value_customers_df)


# COMMAND ----------

high_value_summary_df = (
    gold_customer_360
    .filter(col("`o.total_spend`") >= 10000)
    .agg(
        count("*").alias("high_value_customers"),
        round(sum("`o.total_spend`"), 2).alias("high_value_revenue"),
        round(avg("`o.total_spend`"), 2).alias("avg_high_value_spend")
    )
)

display(high_value_summary_df)
from pyspark.sql.functions import count, sum, avg, round

at_risk_customers_df = (
    gold_customer_360
    .filter(col("customer_risk") == "At Risk")
    .select(
        "customer_id",
        "`o.total_orders`",
        "`o.total_spend`",
        "`p.total_payments`",
        "`w.total_logins`",
        "total_tickets",
        "`cm.campaign_received`",
        "customer_segment",
        "customer_risk"
    )
    .orderBy(desc("`o.total_spend`"))
)

display(at_risk_customers_df)

display(
    final_customer_360_df
    .groupBy("customer_risk")
    .count()
    .orderBy(desc("count"))
)

campaign_effectiveness_df = (
    final_customer_360_df
    .groupBy("`cm.campaign_received`")
    .agg(
        count("*").alias("customers"),
        sum("`o.total_orders`").alias("total_orders"),
        round(sum("`o.total_spend`"), 2).alias("total_spend"),
        round(avg("`o.total_spend`"), 2).alias("avg_spend")
    )
    .orderBy("`cm.campaign_received`")
)

display(campaign_effectiveness_df)

engagement_df = (
    gold_customer_360
    .withColumn(
        "engagement_level",
        when(
            (col("`w.total_logins`") >= 1) &
            (col("`o.total_orders`") >= 2),
            "Highly Engaged"
        )
        .when(
            (col("`w.total_logins`") >= 1) |
            (col("`o.total_orders`") >= 1),
            "Engaged"
        )
        .otherwise("Low Engagement")
    )
)

display(
    engagement_df
    .groupBy("engagement_level")
    .count()
    .orderBy(desc("count"))
)
engagement_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_q.retail_gold.customer_360_analytics")

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, when

gold_customer_360_df = spark.table(
    "retail_q.retail_gold.customer_360_analytics"
)

print("Total customers:", gold_customer_360_df.count())

print(
    "Duplicate customer IDs:",
    gold_customer_360_df
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

# display(
#     gold_customer_360_df.select(
#          "customer_id",
#         "`o.total_orders`",
#         "`o.total_spend`",
#         "`p.total_payments`",
#         "`w.total_logins`",
#         "total_tickets",
#         "`cm.campaign_received`",
#         "customer_segment",
#         "customer_risk",
#         "engagement_level"
#     ).orderBy("customer_id")
# )

display(
    gold_customer_360_df.filter(
        (col("`o.total_orders`") < 0) |
        (col("`p.total_payments`") < 0) |
        (col("`w.total_logins`") < 0) |
        (col("total_tickets") < 0) |
        (col("`cm.campaign_received`") < 0)
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL retail_q.retail_gold.customer_360_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY retail_q.retail_gold.customer_360_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE retail_q.retail_gold.customer_360_analytics
# MAGIC ZORDER BY (customer_id)

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, round

gold_customer_360_df = spark.table(
    "retail_q.retail_gold.customer_360_analytics"
)
customer_segment_summary_df = (
    gold_customer_360_df
    .groupBy("customer_segment")
    .agg(
        count("*").alias("customer_count"),
        sum("`o.total_orders`").alias("total_orders"),
        round(sum("`o.total_spend`"), 2).alias("total_revenue"),
        round(avg("`o.total_spend`"), 2).alias("average_customer_spend"),
        round(avg("total_tickets"), 2).alias("average_tickets")
    )
    .orderBy("customer_segment")
)

display(customer_segment_summary_df)
customer_segment_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_q.retail_gold.customer_segment_summary"
    )