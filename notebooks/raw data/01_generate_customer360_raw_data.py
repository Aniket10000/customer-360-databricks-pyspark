# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

customers = [
    ("C101","Rahul Sharma","rahul@gmail.com","9876543210","Delhi","Delhi","2024-01-15","ACTIVE"),
    ("C102","Priya Singh","priya@gmail.com","9876543211","Mumbai","Maharashtra","2024-02-10","ACTIVE"),
    ("C103","Amit Kumar","amit@gmail.com","9876543212","Pune","Maharashtra","2024-03-05","ACTIVE"),
    ("C104","Sneha Patel","sneha@gmail.com","9876543213","Ahmedabad","Gujarat","2024-03-20","ACTIVE"),
    ("C105","Rohit Verma","rohit@gmail.com","9876543214","Delhi","Delhi","2024-04-11","INACTIVE"),
    ("C106","Neha Gupta","neha@gmail.com","9876543215","Bangalore","Karnataka","2024-05-01","ACTIVE"),
    ("C107","Vikas Yadav","vikas@gmail.com","9876543216","Noida","Uttar Pradesh","2024-05-18","ACTIVE"),
    ("C108","Pooja Mehta","pooja@gmail.com","9876543217","Mumbai","Maharashtra","2024-06-02","ACTIVE"),
    ("C109","Karan Shah","karan@gmail.com","9876543218","Pune","Maharashtra","2024-06-15","ACTIVE"),
    ("C110","Anjali Rao","anji@gmail.com","9876543219","Hyderabad","Telangana","2024-07-01","ACTIVE"),
    ("C111","Manish Jain","manish@gmail.com","9876543220","Jaipur","Rajasthan","2024-07-15","ACTIVE"),
    ("C112","Divya Nair","divya@gmail.com","9876543221","Kochi","Kerala","2024-08-01","ACTIVE"),
    ("C113","Saurabh Mishra","saurabh@gmail.com","9876543222","Lucknow","Uttar Pradesh","2024-08-20","ACTIVE"),
    ("C114","Riya Kapoor","riya@gmail.com","9876543223","Chandigarh","Punjab","2024-09-01","ACTIVE"),
    ("C115","Arjun Malhotra","arjun@gmail.com","9876543224","Gurgaon","Haryana","2024-09-15","ACTIVE"),

    # DQ issue: duplicate customer
    ("C101","Rahul Sharma","rahul@gmail.com","9876543210","Delhi","Delhi","2024-01-15","ACTIVE"),

    # DQ issue: null customer_id
    (None,"Test Customer","test@gmail.com","9876543225","Delhi","Delhi","2024-10-01","ACTIVE"),

    # DQ issue: invalid email
    ("C116","Invalid Email","invalid-email","9876543226","Delhi","Delhi","2024-10-02","ACTIVE")
]

customer_schema = """
customer_id STRING,
customer_name STRING,
email STRING,
phone STRING,
city STRING,
state STRING,
signup_date STRING,
customer_status STRING
"""

customers_df = spark.createDataFrame(customers, customer_schema)


customers_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/customers/")

# COMMAND ----------

# order data

orders = [
    ("O1001","C101","2026-08-01",5000,"COMPLETED"),
    ("O1002","C102","2026-08-03",2500,"COMPLETED"),
    ("O1003","C101","2026-08-05",8000,"COMPLETED"),
    ("O1004","C103","2026-08-07",1500,"CANCELLED"),
    ("O1005","C104","2026-08-10",7200,"COMPLETED"),
    ("O1006","C105","2026-08-11",3200,"COMPLETED"),
    ("O1007","C106","2026-08-12",4500,"SHIPPED"),
    ("O1008","C101","2026-08-15",9000,"COMPLETED"),
    ("O1009","C107","2026-08-16",1200,"COMPLETED"),
    ("O1010","C108","2026-08-18",6500,"COMPLETED"),
    ("O1011","C109","2026-08-20",2100,"CANCELLED"),
    ("O1012","C110","2026-08-21",3500,"COMPLETED"),
    ("O1013","C111","2026-08-22",15000,"COMPLETED"),
    ("O1014","C112","2026-08-23",1800,"COMPLETED"),
    ("O1015","C113","2026-08-24",4000,"COMPLETED"),
    ("O1016","C114","2026-08-25",5600,"SHIPPED"),
    ("O1017","C115","2026-08-26",11000,"COMPLETED"),
    ("O1018","C101","2026-08-27",12500,"COMPLETED"),
    ("O1019","C102","2026-08-28",1700,"COMPLETED"),
    ("O1020","C103","2026-08-29",2200,"COMPLETED"),
    ("O1021","C104","2026-08-30",3500,"COMPLETED"),
    ("O1022","C106","2026-08-31",4800,"COMPLETED"),
    ("O1023","C107","2026-09-01",7000,"COMPLETED"),
    ("O1024","C108","2026-09-02",2600,"COMPLETED"),
    ("O1025","C109","2026-09-03",9000,"COMPLETED"),
    ("O1026","C110","2026-09-04",3000,"COMPLETED"),
    ("O1027","C111","2026-09-05",5500,"COMPLETED"),
    ("O1028","C112","2026-09-06",1600,"COMPLETED"),
    ("O1029","C999","2026-09-07",4500,"COMPLETED"),
    ("O1030","C101","2026-09-08",-500,"COMPLETED")
]

order_schema = """
order_id STRING,
customer_id STRING,
order_date STRING,
order_amount DOUBLE,
order_status STRING
"""

orders_df = spark.createDataFrame(orders, order_schema)

orders_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/orders/")

# COMMAND ----------

# payments dataset
payments = [
    ("P2001","O1001","C101","2026-08-01",5000,"UPI","SUCCESS"),
    ("P2002","O1002","C102","2026-08-03",2500,"CARD","SUCCESS"),
    ("P2003","O1003","C101","2026-08-05",8000,"CARD","SUCCESS"),
    ("P2004","O1004","C103","2026-08-07",1500,"UPI","FAILED"),
    ("P2005","O1005","C104","2026-08-10",7200,"CARD","SUCCESS"),
    ("P2006","O1006","C105","2026-08-11",3200,"NETBANKING","SUCCESS"),
    ("P2007","O1007","C106","2026-08-12",4500,"UPI","SUCCESS"),
    ("P2008","O1008","C101","2026-08-15",9000,"CARD","SUCCESS"),
    ("P2009","O1009","C107","2026-08-16",1200,"UPI","SUCCESS"),
    ("P2010","O1010","C108","2026-08-18",6500,"CARD","SUCCESS"),
    ("P2011","O1011","C109","2026-08-20",2100,"UPI","FAILED"),
    ("P2012","O1012","C110","2026-08-21",3500,"CARD","SUCCESS"),
    ("P2013","O1013","C111","2026-08-22",15000,"NETBANKING","SUCCESS"),
    ("P2014","O1014","C112","2026-08-23",1800,"UPI","SUCCESS"),
    ("P2015","O1015","C113","2026-08-24",4000,"CARD","SUCCESS"),
    ("P2016","O1016","C114","2026-08-25",5600,"UPI","SUCCESS"),
    ("P2017","O1017","C115","2026-08-26",11000,"CARD","SUCCESS"),
    ("P2018","O1018","C101","2026-08-27",12500,"CARD","SUCCESS"),
    ("P2019","O1019","C102","2026-08-28",1700,"UPI","SUCCESS"),
    ("P2020","O1020","C103","2026-08-29",2200,"UPI","SUCCESS"),

    # DQ: duplicate payment
    ("P2001","O1001","C101","2026-08-01",5000,"UPI","SUCCESS"),

    # DQ: unknown order
    ("P2022","O9999","C101","2026-09-01",3000,"CARD","SUCCESS"),

    # DQ: negative amount
    ("P2023","O1021","C104","2026-08-30",-3500,"CARD","SUCCESS"),

    # DQ: invalid status
    ("P2024","O1022","C106","2026-08-31",4800,"UPI","UNKNOWN"),
]

payment_schema = """
payment_id STRING,
order_id STRING,
customer_id STRING,
payment_date STRING,
amount DOUBLE,
payment_method STRING,
payment_status STRING
"""

payments_df = spark.createDataFrame(payments, payment_schema)

payments_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/payments/")

# COMMAND ----------

#  web event dataset
web_events = [
    ("E3001","C101","LOGIN","2026-08-01 09:10:00","home","MOBILE"),
    ("E3002","C101","PRODUCT_VIEW","2026-08-01 09:12:00","electronics","MOBILE"),
    ("E3003","C101","ADD_TO_CART","2026-08-01 09:15:00","electronics","MOBILE"),
    ("E3004","C102","LOGIN","2026-08-03 10:00:00","home","WEB"),
    ("E3005","C102","PRODUCT_VIEW","2026-08-03 10:05:00","fashion","WEB"),
    ("E3006","C102","CHECKOUT","2026-08-03 10:20:00","cart","WEB"),
    ("E3007","C103","LOGIN","2026-08-07 11:00:00","home","MOBILE"),
    ("E3008","C103","PRODUCT_VIEW","2026-08-07 11:05:00","books","MOBILE"),
    ("E3009","C104","LOGIN","2026-08-10 12:00:00","home","WEB"),
    ("E3010","C104","PRODUCT_VIEW","2026-08-10 12:05:00","electronics","WEB"),
    ("E3011","C104","ADD_TO_CART","2026-08-10 12:10:00","electronics","WEB"),
    ("E3012","C105","LOGIN","2026-08-11 13:00:00","home","MOBILE"),
    ("E3013","C106","LOGIN","2026-08-12 14:00:00","home","WEB"),
    ("E3014","C106","PRODUCT_VIEW","2026-08-12 14:05:00","fashion","WEB"),
    ("E3015","C106","CHECKOUT","2026-08-12 14:20:00","cart","WEB"),
    ("E3016","C107","LOGIN","2026-08-16 15:00:00","home","MOBILE"),
    ("E3017","C107","PRODUCT_VIEW","2026-08-16 15:05:00","electronics","MOBILE"),
    ("E3018","C108","LOGIN","2026-08-18 16:00:00","home","WEB"),
    ("E3019","C108","ADD_TO_CART","2026-08-18 16:10:00","electronics","WEB"),
    ("E3020","C109","LOGIN","2026-08-20 17:00:00","home","MOBILE"),
    ("E3021","C109","PRODUCT_VIEW","2026-08-20 17:05:00","books","MOBILE"),
    ("E3022","C110","LOGIN","2026-08-21 18:00:00","home","WEB"),
    ("E3023","C110","CHECKOUT","2026-08-21 18:15:00","cart","WEB"),
    ("E3024","C111","LOGIN","2026-08-22 09:00:00","home","MOBILE"),
    ("E3025","C111","PRODUCT_VIEW","2026-08-22 09:05:00","electronics","MOBILE"),
    ("E3026","C111","ADD_TO_CART","2026-08-22 09:10:00","electronics","MOBILE"),
    ("E3027","C112","LOGIN","2026-08-23 10:00:00","home","WEB"),
    ("E3028","C113","LOGIN","2026-08-24 11:00:00","home","MOBILE"),
    ("E3029","C113","PRODUCT_VIEW","2026-08-24 11:05:00","fashion","MOBILE"),
    ("E3030","C114","LOGIN","2026-08-25 12:00:00","home","WEB"),
    ("E3031","C114","PRODUCT_VIEW","2026-08-25 12:05:00","electronics","WEB"),
    ("E3032","C115","LOGIN","2026-08-26 13:00:00","home","MOBILE"),
    ("E3033","C115","CHECKOUT","2026-08-26 13:20:00","cart","MOBILE"),

    # DQ: invalid event
    ("E3034","C101","UNKNOWN","2026-08-27 09:00:00","home","WEB"),

    # DQ: unknown customer
    ("E3035","C999","PRODUCT_VIEW","2026-08-27 09:10:00","electronics","WEB"),

    # DQ: null customer
    ("E3036",None,"LOGIN","2026-08-28 10:00:00","home","MOBILE"),

    # DQ: duplicate event
    ("E3001","C101","LOGIN","2026-08-01 09:10:00","home","MOBILE"),
]

web_schema = """
event_id STRING,
customer_id STRING,
event_type STRING,
event_timestamp STRING,
page STRING,
device STRING
"""

web_events_df = spark.createDataFrame(web_events, web_schema)

web_events_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/web_events")

# COMMAND ----------

#  campaigns dataset

campaigns = [
    ("CAMP501","C101","Diwali Sale","2026-08-01","EMAIL","CLICKED"),
    ("CAMP502","C102","Monsoon Sale","2026-08-03","SMS","IGNORED"),
    ("CAMP503","C103","Mega Discount","2026-08-05","EMAIL","CLICKED"),
    ("CAMP504","C104","Diwali Sale","2026-08-07","PUSH","CLICKED"),
    ("CAMP505","C105","Monsoon Sale","2026-08-10","EMAIL","IGNORED"),
    ("CAMP506","C106","Mega Discount","2026-08-12","SMS","CLICKED"),
    ("CAMP507","C107","Diwali Sale","2026-08-15","EMAIL","IGNORED"),
    ("CAMP508","C108","Monsoon Sale","2026-08-18","PUSH","CLICKED"),
    ("CAMP509","C109","Mega Discount","2026-08-20","EMAIL","IGNORED"),
    ("CAMP510","C110","Diwali Sale","2026-08-21","SMS","CLICKED"),
    ("CAMP511","C111","Monsoon Sale","2026-08-22","EMAIL","CLICKED"),
    ("CAMP512","C112","Mega Discount","2026-08-23","PUSH","IGNORED"),
    ("CAMP513","C113","Diwali Sale","2026-08-24","EMAIL","CLICKED"),
    ("CAMP514","C114","Monsoon Sale","2026-08-25","SMS","IGNORED"),
    ("CAMP515","C115","Mega Discount","2026-08-26","PUSH","CLICKED"),

    # DQ: unknown customer
    ("CAMP516","C999","Diwali Sale","2026-08-27","EMAIL","CLICKED"),

    # DQ: invalid response
    ("CAMP517","C101","Mega Discount","2026-08-28","SMS","UNKNOWN")
]

campaign_schema = """
campaign_id STRING,
customer_id STRING,
campaign_name STRING,
campaign_date STRING,
channel STRING,
response STRING
"""

campaigns_df = spark.createDataFrame(campaigns, campaign_schema)

campaigns_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/campaigns")

# COMMAND ----------

#  support ticket

tickets = [
    ("T4001","C101","2026-08-02","PAYMENT","HIGH","RESOLVED",4.5),
    ("T4002","C102","2026-08-04","DELIVERY","MEDIUM","RESOLVED",12.0),
    ("T4003","C103","2026-08-08","PRODUCT","LOW","OPEN",0.0),
    ("T4004","C104","2026-08-11","PAYMENT","HIGH","RESOLVED",3.5),
    ("T4005","C105","2026-08-12","DELIVERY","MEDIUM","OPEN",0.0),
    ("T4006","C106","2026-08-13","PRODUCT","LOW","RESOLVED",8.0),
    ("T4007","C107","2026-08-17","PAYMENT","HIGH","RESOLVED",2.0),
    ("T4008","C108","2026-08-19","DELIVERY","MEDIUM","RESOLVED",15.0),
    ("T4009","C109","2026-08-21","PRODUCT","LOW","OPEN",0.0),
    ("T4010","C110","2026-08-22","PAYMENT","HIGH","RESOLVED",5.0),
    ("T4011","C111","2026-08-23","DELIVERY","MEDIUM","RESOLVED",7.0),
    ("T4012","C112","2026-08-24","PRODUCT","LOW","OPEN",0.0),
    ("T4013","C113","2026-08-25","PAYMENT","HIGH","RESOLVED",4.0),
    ("T4014","C114","2026-08-26","DELIVERY","MEDIUM","RESOLVED",10.0),
    ("T4015","C999","2026-08-27","PAYMENT","HIGH","OPEN",0.0),

    # DQ: invalid priority
    ("T4016","C115","2026-08-28","PRODUCT","URGENT","OPEN",0.0)
]

ticket_schema = """
ticket_id STRING,
customer_id STRING,
ticket_date STRING,
issue_type STRING,
priority STRING,
ticket_status STRING,
resolution_time_hours DOUBLE
"""

tickets_df = spark.createDataFrame(tickets, ticket_schema)

tickets_df.write.mode("overwrite").option("header",True).csv("/Volumes/retail_q/volumes/customer360_raw/support_tickets/")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/retail_q/volumes/customer360_raw/"))

print("Customers:", customers_df.count())
print("Orders:", orders_df.count())
print("Payments:", payments_df.count())
print("Web Events:", web_events_df.count())
print("Tickets:", tickets_df.count())
print("Campaigns:", campaigns_df.count())

# COMMAND ----------



customer_df = spark.read.csv("/Volumes/retail_q/volumes/customer360_raw/customers/", header=True, inferSchema=True).withColumn("ingestion_timestamp", F.current_timestamp())

customer_df.write.format("delta").mode("overwrite").saveAsTable("retail_q.customer360_bronze.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_q.customer360_bronze.customers;

# COMMAND ----------

# Function to ingest the data

def ingest_to_bronze(source, table_name):

    source_path = f"/Volumes/retail_q/volumes/customer360_raw/{source}/"
    target_table = f"retail_q.customer360_bronze.{table_name}"

    df = spark.read.csv(source_path, header=True, inferSchema=True).withColumn("ingestion_timestamp", F.current_timestamp())

    df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    print(f"Count of table is {df.count()}")






# COMMAND ----------

ingest_to_bronze("orders", "orders")
ingest_to_bronze("payments", "payments")
ingest_to_bronze("web_events", "web_events")
ingest_to_bronze("support_tickets", "support_tickets")
ingest_to_bronze("campaigns", "campaigns")


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retail_q.customer360_bronze;
# MAGIC
# MAGIC SELECT 'customers' AS table_name, COUNT(*) AS cnt
# MAGIC FROM retail_q.customer360_bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', COUNT(*)
# MAGIC FROM retail_q.customer360_bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments', COUNT(*)
# MAGIC FROM retail_q.customer360_bronze.payments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'web_events', COUNT(*)
# MAGIC FROM retail_q.customer360_bronze.web_events
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'support_tickets', COUNT(*)
# MAGIC FROM retail_q.customer360_bronze.support_tickets
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'campaigns', COUNT(*)
# MAGIC FROM retail_q.customer360_bronze.campaigns;