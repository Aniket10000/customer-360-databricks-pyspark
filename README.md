# customer-360-databricks-pyspark
The project follows the Medallion Architecture (Bronze → Silver → Gold) and demonstrates data ingestion, data-quality validation, deduplication, referential-integrity checks, quarantine handling, customer-level aggregations, business segmentation, risk classification, engagement analysis, and Databricks SQL analytics

The objective is to create a unified customer-level view by integrating data from multiple business domains such as:

Customers
Orders
Payments
Web Events
Support Tickets
Marketing Campaigns

The project follows the Medallion Architecture (Bronze → Silver → Gold) and demonstrates data ingestion, data-quality validation, deduplication, referential-integrity checks, quarantine handling, customer-level aggregations, business segmentation, risk classification, engagement analysis, and Databricks SQL analytics.

🎯 Business Objective

Organizations often have customer information distributed across multiple systems.

For example:

Customer System
       ↓
Order System
       ↓
Payment System
       ↓
Web/Application Events
       ↓
Support System
       ↓
Marketing System

The objective of this project is to bring these datasets together and create a single customer-level view.

The final Customer 360 dataset answers questions such as:

How many orders has a customer placed?
How much has the customer spent?
How many payments has the customer made?
How active is the customer?
How many support tickets has the customer raised?
Has the customer received a campaign?
Which customer segment does the customer belong to?
What is the customer's risk category?
What is the customer's engagement level?
🏗️ Architecture

The project follows the Medallion Architecture:

                         RAW DATA
                            │
                            ▼
                    ┌───────────────┐
                    │    BRONZE     │
                    │   Raw Delta   │
                    └───────┬───────┘
                            │
                            ▼
                    ┌───────────────┐
                    │    SILVER     │
                    │               │
                    │ Data Cleaning │
                    │ Data Quality  │
                    │ Deduplication │
                    │ Validation    │
                    └───────┬───────┘
                            │
                    ┌───────┴────────┐
                    │                │
                    ▼                ▼
              Valid Records      Rejected Records
                    │                │
                    │          Quarantine Tables
                    │
                    ▼
                    ┌───────────────┐
                    │     GOLD      │
                    │ Customer 360  │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
          Segment          Risk       Engagement
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                    Business Analytics
                            │
                            ▼
                   Databricks SQL
                      Dashboard
🛠️ Technologies Used
Technology	Purpose
Databricks	Data engineering and analytics platform
PySpark	Data transformation and processing
Apache Spark	Distributed data processing
Delta Lake	Reliable storage and transactional tables
SQL	Analytics and reporting
Python	Data transformation
Medallion Architecture	Data-layer organization
📂 Source Datasets

The project uses six primary datasets.

1. Customers

Contains customer master information.

Example attributes:

customer_id
customer_name
email
customer_type
2. Orders

Contains customer order information.

Example attributes:

order_id
customer_id
order_date
order_amount
order_status
3. Payments

Contains customer payment information.

Example attributes:

payment_id
customer_id
order_id
payment_amount
payment_date
payment_status
4. Web Events

Contains customer application/web activity.

Example attributes:

event_id
customer_id
event_type
event_timestamp

Examples of events:

LOGIN
CHECKOUT
5. Support Tickets

Contains customer support interactions.

Example attributes:

ticket_id
customer_id
ticket_date
ticket_status
6. Campaigns

Contains marketing campaign information.

Example attributes:

campaign_id
customer_id
campaign_date
campaign_type
🥉 Bronze Layer

The Bronze layer stores the source data in Delta format with minimal transformation.

The objective of Bronze is to preserve the incoming source data and provide a reliable starting point for downstream processing.

Bronze responsibilities
Raw data ingestion
Schema definition/inference
Conversion to Delta
Adding ingestion metadata where applicable
Preserving source information

Example:

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(bronze_table)
🥈 Silver Layer

The Silver layer contains cleaned, standardized and validated data.

The following data-quality rules were implemented.

Data Quality Checks
1. Null Validation

Required business keys and important attributes were checked for null values.

Examples:

customer_id
order_id
order_date

Invalid records were moved to quarantine.

2. Duplicate Handling

Duplicate records were identified and removed based on the appropriate business key.

This prevents duplicate records from affecting downstream aggregations.

3. Business Rule Validation

Business-specific rules were applied.

Examples:

order_amount > 0
quantity > 0
valid order status
valid payment information
4. Referential Integrity

Relationships between datasets were validated.

For example:

Orders
   │
   └── customer_id
             │
             ▼
        Silver Customers

An order referencing a customer that does not exist in the valid Customer Silver dataset is rejected and moved to quarantine.

5. Quarantine Handling

Invalid records are not simply deleted.

They are stored separately with information such as:

customer_id
rejection_reason
rejection_timestamp

This provides traceability and allows rejected records to be investigated or reprocessed.

🥇 Gold Layer

The Gold layer contains business-ready and analytics-ready datasets.

The primary Gold table is:

retail_q.retail_gold.customer_360

The project also creates analytics-oriented Gold tables such as:

retail_q.retail_gold.customer_360_analytics

retail_q.retail_gold.customer_segment_summary
👤 Customer 360 Metrics

The final Customer 360 profile contains customer-level metrics including:

Metric	Description
total_orders	Total number of customer orders
total_spend	Total customer spending
total_payments	Number of payments
total_logins	Number of login events
total_tickets	Number of support tickets
campaign_received	Campaign participation indicator
customer_segment	Business-defined customer segment
customer_risk	Customer risk classification
engagement_level	Customer engagement classification
⚠️ Important Customer 360 Design Consideration

One important issue when building a Customer 360 pipeline is the one-to-many join multiplication problem.

For example, suppose a customer has:

3 Orders
4 Payments
5 Web Events

Joining all three datasets directly can produce:

3 × 4 × 5 = 60 rows

This can incorrectly inflate aggregations.

To avoid this, each one-to-many dataset is first aggregated at the customer level:

Orders
   ↓
customer-level aggregation

Payments
   ↓
customer-level aggregation

Web Events
   ↓
customer-level aggregation

Tickets
   ↓
customer-level aggregation

Campaigns
   ↓
customer-level aggregation

          ↓
       JOIN

          ↓

    Customer 360

This ensures that the final metrics are not artificially inflated.

📊 Customer Segmentation

Customers are classified into business segments using customer-level metrics.

The current dataset produced:

Segment	Customers	Orders	Revenue	Average Spend
High	2	6	₹55,000	₹27,500
Mid	2	3	₹21,700	₹10,850
Regular	11	19	₹54,600	₹4,963.64

The segmentation logic is implemented in the Gold transformation.

Note: Segment thresholds are business-defined rules for this project and can be changed based on business requirements.

🚦 Customer Risk

The project derives a customer risk classification from the available Customer 360 metrics.

Current dataset:

Risk	Customers
Regular	11
Mid	4

The current sample does not contain customers classified into a higher-risk category.

📱 Customer Engagement

An engagement level is derived using customer activity metrics such as:

Login activity
Order activity

This produces an additional business dimension that can be used for customer analysis.

📈 Analytics

The project includes analytics such as:

Customer Segment Distribution
High       2
Mid        2
Regular   11
Revenue by Segment
High       ₹55,000
Regular    ₹54,600
Mid        ₹21,700
Overall Metrics
Total Customers       = 15
Total Orders          = 28
Total Revenue         = ₹131,300
Average Customer Spend = ₹8,753.33
📊 Databricks SQL Dashboard

A dashboard was created using the Gold-layer data.

The dashboard contains:

KPI Cards
Total Customers
Total Revenue
Total Orders
Average Customer Spend
Charts
Revenue by Customer Segment
Customer Distribution by Segment
Customer Risk Distribution
Customer Engagement
Detail Table

Top customers by:

total_spend

along with:

customer_segment
customer_risk
engagement_level
⚡ Delta Lake & Performance

The Gold tables are stored using Delta Lake.

Delta provides capabilities such as:

ACID transactions
Schema enforcement
Schema evolution
Time travel
MERGE/UPSERT support
Reliable concurrent data operations

The Gold analytics table can be optimized using:

OPTIMIZE retail_q.retail_gold.customer_360_analytics
ZORDER BY (customer_id);

OPTIMIZE helps with file compaction, while ZORDER can improve data skipping for queries that frequently filter on the selected column.

🔍 Data Validation Results

The final Customer 360 dataset was validated before being used for analytics.

Validation results:

Total Customers       : 15
Duplicate Customer IDs: 0
Negative Metrics      : 0

The final dataset therefore passed the implemented validation checks.

📁 Project Structure
customer-360-databricks-pyspark/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── analytics/
│
├── data/
│   └── README.md
│
├── screenshots/
│   ├── customer_360.png
│   ├── segment_summary.png
│   └── dashboard.png
│
├── architecture/
│   └── customer_360_architecture.png
│
└── README.md

Update the folder and notebook names above to match the actual files in this repository.

🚀 How to Run
Prerequisites

You need access to:

Databricks workspace
Databricks cluster or SQL Warehouse
PySpark
Delta Lake
Steps
1. Clone the repository
git clone <your-repository-url>
2. Import the notebooks into Databricks

Upload/import the notebooks from the notebooks directory.

3. Upload the source datasets

Place the sample datasets in the required Databricks Volume or storage location.

4. Run the notebooks in order
01 → Bronze Ingestion
02 → Silver Customer
03 → Silver Orders
04 → Silver Payments
05 → Silver Web Events
06 → Silver Tickets
07 → Silver Campaigns
08 → Customer 360 Gold
09 → Analytics
10 → Dashboard

The exact execution order may vary depending on the notebook structure.

🧠 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

PySpark DataFrames
Spark SQL
select()
withColumn()
when() / otherwise()
filter()
groupBy()
Aggregations
Joins
Anti joins
Window functions
Deduplication
Null handling
Data-quality checks
Referential integrity
Quarantine/reject handling
Delta Lake
Medallion Architecture
Databricks SQL
OPTIMIZE
ZORDER
Customer-level data modeling
💡 Key Learnings
1. Data Quality is part of the pipeline

Invalid records should be identified and handled explicitly rather than silently removed.

2. Aggregation before joining is important

When multiple one-to-many datasets are involved, aggregating them before joining prevents metric inflation.

3. Bronze should preserve source data

The Bronze layer should remain as close to the source as practical, while transformation and business rules are applied downstream.

4. Gold should answer business questions

The purpose of the Gold layer is not simply to store transformed data. It should provide data that can directly support analytics and reporting.

5. Delta Lake improves reliability

Using Delta provides transactional capabilities and features useful for maintaining production data pipelines.
