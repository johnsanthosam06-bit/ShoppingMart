
## 📌 Project Overview
# E-commerce Business Performance & Customer Engagement Analysis
A mid-sized retail business, “ShoppingMart”, is experiencing significant competition. They need deeper insights into customer behaviors, sales trend, inventory management, online engagement, and product sentiment to strategically boost revenues and enhance customer satisfaction.

Objective:
Utilize structured (transactional and inventory) and unstructured data (social media, online reviews, web logs) to build a unified analytics solution that delivers actionable insights and tracks key business KPIs.

## 🛠 Tools Used
- Microsoft Fabric
- Power BI
- Warehouse
- Pyspark
- JSON configuration
- Lakehouse

## 📊 Project Architecture
1. Data Ingestion
2. Data Transformation
3. Semantic Model Creation
4. Power BI Reporting

## 📁 Project Files
- Pyspark scripts for transformation
- Power BI dashboard (.pbix)

# Dashboard Overview :


<img width="2130" height="1412" alt="image" src="https://github.com/user-attachments/assets/0a00520d-986a-46a0-b419-dde52d3f7e81" />



 ## Bronze Layer – Structured Data Ingestion
The Bronze layer is responsible for ingesting raw structured data into Microsoft Fabric Lakehouse without applying any transformations.
A Data Pipeline named PL_Data_Ingest is created to automate the ingestion process.
The pipeline works as follows:
A Lookup activity reads the list of available files from the raw data location.
A ForEach activity iterates through each file dynamically.
Inside the loop, a Copy activity transfers each file from the raw source to the Lakehouse (Files section – Bronze folder).
The Bronze layer stores data in its original format and structure. No cleansing or transformation is applied at this stage. This layer acts as the raw data foundation and ensures traceability for downstream processing in the Silver layer.

<img width="1488" height="616" alt="image" src="https://github.com/user-attachments/assets/cb7c004a-60c9-4204-8f9e-b58b87139bf1" />

## Bronze Layer – Unstructured Data Ingestion
The Bronze layer also handles ingestion of unstructured data such as social media data, online reviews, and web logs into Microsoft Fabric Lakehouse without applying transformations.
A Data Pipeline (using the same ingestion framework) is used to automate this process.
The pipeline flow is as follows:
A Lookup activity retrieves the list of unstructured files from the raw data source.
A ForEach activity dynamically iterates through each file.
Inside the loop, a Copy activity moves the files from the raw location to the Lakehouse (Files section – Bronze folder).
At this stage, the data is stored in its original format (e.g., JSON, text, logs) without cleansing or processing. The Bronze layer preserves raw unstructured data for further processing, transformation, and analysis in the Silver layer.

<img width="1252" height="508" alt="image" src="https://github.com/user-attachments/assets/7ef92db4-7ebd-4ae7-9b68-a3767b20a5f7" />


## Bronze Layer – Parquet Standardization Using Notebook:

After the pipeline ingests raw files into the Lakehouse (Files folder), a Fabric Notebook (PySpark) is used to read the ingested data directly from the Bronze location. The data is loaded into Spark DataFrames and written back in Parquet format using overwrite mode.
The Parquet files are stored in designated Lakehouse paths (e.g., Files/ShoppingMart_Silver_reviews, Files/ShoppingMart_Silver_social, Files/ShoppingMart_Silver_weblogs).
This step standardizes the data into an optimized columnar format, improving performance and preparing it for further validation and transformation in the Silver layer.



```
from pyspark.sql.functions import *
df_customers = spark.read.format("json").option("header","true").load("Files/ShoppingMart_Bronze_Customers/ShoppingMart_customers.csv")
df_Orders = spark.read.format("json").option("header","true").load("Files/ShoppingMart_Bronze_Orders/ShoppingMart_orders.csv")
df_Products = spark.read.format("json").option("header","true").load("Files/ShoppingMart_Bronze_Products/ShoppingMart_products.csv")
df_reviews = spark.read.json("Files/ShoppingMart_Bronze_Reviews/ShoppingMart_review.json")
df_social= spark.read.json("Files/ShoppingMart_Bronze_Social_Media/ShoppingMart_social_media.json")
df_web = spark.read.json("Files/ShoppingMart_Bronze_Web_Logs/ShoppingMart_web_logs.json")

display(df_web)

df_Orders = df_Orders.dropna(subset=['OrderId','ProductID','CustomerID','OrderDate','TotalAmount'])
df_Orders = df_Orders.withColumn("OrderDate",to_date("OrderDate"))

display(df_Orders)

df_Orders = df_Orders.join(df_Products,on = 'ProductID',how="inner")\
.join(df_customers,on = 'CustomerID',how="inner")

display(df_Orders)

df_Orders.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Customers/ShoppingMart_customer_Ordersdata")
df_reviews.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_reviews/ShoppingMart_reviews")
df_social.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_social/ShoppingMart_social_media")
df_web.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_weblogs/ShoppingMart_web_logs")


```

### Pipeline for Raw To Landing
A ForEach activity is used in the Fabric Pipeline to dynamically iterate over all files available in the Raw folder. The pipeline fetches the file list and executes the Notebook for each file individually, enabling automated and scalable data ingestion.

This design supports batch file processing and eliminates the need for hardcoded file references.

<img width="1184" height="568" alt="image" src="https://github.com/user-attachments/assets/ecf6f046-28eb-42bf-901e-709da307d6af" />


 ## Silver Layer – Transformations & Aggregations Using Notebook
Before moving to Power BI, a Fabric Notebook (PySpark) is used to perform required data transformations and aggregations on the standardized Parquet files.
A Lakehouse shortcut is created to easily access the previously generated Parquet files. The notebook reads these Parquet files into Spark DataFrames and performs analytical transformations.
Key transformations include:
Web Logs: Aggregated using groupBy to measure user engagement per page and action.
Social Media Data: Aggregated to track sentiment trends and overall engagement patterns.
Product Reviews: Grouped by product to calculate average rating and review metrics.
After performing the required transformations and aggregations, the processed data is written back in Parquet format to a new Lakehouse file location.
These Parquet files are then loaded as Lakehouse Tables using the “Load to Table” option, making them available under the Tables section for reporting and Power BI integration.
This step prepares clean, structured, and business-ready data for the Gold layer and reporting.

```
from pyspark.sql.functions import *

customer_df = spark.read.parquet("Files/ShoppingMart_Silver_Customers/ShoppingMart_customer_Ordersdata")
reviews_df = spark.read.parquet("Files/ShoppingMart_Silver_reviews/ShoppingMart_reviews")
social_df = spark.read.parquet("Files/ShoppingMart_Silver_social/ShoppingMart_social_media")
web_df = spark.read.parquet("Files/ShoppingMart_Silver_weblogs/ShoppingMart_web_logs")

display(web_df)


# Aggregates web log data to measure engagement per user on each page and action

web_df = web_df.groupBy("user_id","page","action").count()
web_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_weblogs/ShoppingMart_web_logs")
display(web_df)

#Aggregate social media data to track sentiment trends accross 

social_df = social_df.groupBy("platform","sentiment").count()
social_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_social/ShoppingMart_social_media")

display(social_df)

#Aggregate product reviews to calculate average rating per product
reviews_df = spark.read.parquet("Files/ShoppingMart_Silver_reviews/ShoppingMart_reviews")

reviews_df = reviews_df.groupBy("product_id").agg(avg("rating").alias("AvgRating"))
reviews_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_reviews/ShoppingMart_reviews")
display(reviews_df)

customer_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Customers/ShoppingMart_customer_Ordersdata")


```



##  📊 Semantic Model & Dashboard Creation
After preparing the transformed tables in the Lakehouse, a Semantic Model was created to enable reporting and analytics.
The semantic model includes the following tables:
* Customer Table
* Review Table
* Web Engagement Table
* Social Media Aggregated Table
Additionally, a dedicated Date Table was created to support time-based analysis (Day, Month, Month Short, Year).
Using the Manage Relationships option, appropriate relationships were established between fact tables and dimension tables (Customer and Date), forming a structured star-schema-like model. This ensures proper filtering, drill-down capability, and accurate aggregations across reports.
Once the relationships were configured, the model was used to build an interactive Power BI dashboard, enabling analysis of:
Product average ratings
Customer engagement metrics
Web activity trends
Social media sentiment insights
Time-based performance analysis
This semantic layer acts as the business-ready data model that powers reporting and KPI tracking.

<img width="1874" height="704" alt="image" src="https://github.com/user-attachments/assets/7afc98f8-4435-4bda-b559-b971afbcee92" />

## 📈 Interactive Dashboard – Shopping Mart Analytics KPI Metrics:

The final phase of the project involved designing and deploying an interactive Power BI dashboard powered by the curated semantic model. This dashboard serves as a centralized analytics layer, transforming processed data into actionable business intelligence.
Report Capabilities
The report incorporates dynamic slicers (Year, Month, Day, Product, and Category) to enable comprehensive cross-filtering across all visuals. Any selection automatically propagates throughout the report, recalculating KPIs, rankings, trend analysis, and category performance in real time. This ensures analytical consistency and an intuitive user experience.
Core Analytical Components:
Performance & Revenue Insights:
* Top 5 Products by Sales
* Top 5 Customers by Sales
* Sales by Category (Clustered Bar Chart):
* Monthly Sales Trend (Line Chart)
These visuals highlight revenue drivers, customer contribution, and performance trends over time.
Product & Inventory Intelligence: 
* Top 5 Products by Reviews
* Top 5 Products by Stock Quantity
These insights support product performance evaluation and inventory monitoring.
Engagement & Sentiment Analytics:
* Social Sentiment Distribution (Pie Chart)
* Web Engagement Analysis (Pie Chart)
These components integrate unstructured data insights, providing visibility into customer interaction patterns and brand perception trends.
Executive KPIs:
* Total Sales ($)
* Total Products Sold
These KPI indicators provide high-level performance metrics for quick executive review.


<img width="2130" height="1412" alt="image" src="https://github.com/user-attachments/assets/0a00520d-986a-46a0-b419-dde52d3f7e81" />

