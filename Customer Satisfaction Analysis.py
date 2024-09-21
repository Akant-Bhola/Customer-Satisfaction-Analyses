# Databricks notebook source
# MAGIC %md
# MAGIC # **Analyzing Customer Satisfaction Trends in 2022**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Introduction:**
# MAGIC
# MAGIC This notebook aims to analyze customer satisfaction trends across various regions, products and case owners during first quarter of the year 2022. The provided dataset consists of several attributes, including customer satisfaction scores, product information and case details. The objective of this analysis is to identify factors contributing to a downward trend in the Overall Technician Satisfaction rating starting in January and to provide actionable recommendations to improve these ratings.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Loading, Transformations and Initial Inspection:**
# MAGIC
# MAGIC To begin our analysis, we load the customer satisfaction dataset into a Spark DataFrame from AWS S3. We perform some necessary data transformations to standardize certain fields, ensuring consistency in our analysis. For example, country names are unified and the language of the Issue_Resolved field is standardized to English. These transformations will help in the subsequent analysis, particularly in regional and issue resolution impact studies.
# MAGIC
# MAGIC After performing these transformations, we print the schema of the DataFrame to understand the data types of each column. Finally, the first 10 rows of the dataset are displayed to inspect the data structure, giving us an overview of the key attributes available for our analysis.

# COMMAND ----------

# Import data from s3 AWS
df = spark.read.option("sep", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3n://humber-lfb-databricks-class-files/midterm_helpdesk.csv")
# "sep" is used to specify the delimiter of CSV file, "header" is used to specify the first row is a header, and "inferSchema" is used to automatically infer the schema

# Display the updated DataFrame
display(df)

# COMMAND ----------

#To check the schema of the data
df.printSchema()

# COMMAND ----------

# Transformations
from pyspark.sql.functions import when

# Replace "United States" with "USA" in the Country column
df = df.withColumn("Country", when(df.Country == "United States", "USA").otherwise(df.Country))

# Replace "Oui" with "Yes" and "Non" with "No" in the Issue_Resolved column
df = df.withColumn("Issue_Resolved", when(df.Issue_Resolved == "Oui", "Yes").otherwise(df.Issue_Resolved))
df = df.withColumn("Issue_Resolved", when(df.Issue_Resolved == "Non", "No").otherwise(df.Issue_Resolved))

# Display the updated DataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Overall Technician Satisfaction over Time:**
# MAGIC
# MAGIC The line chart illustrates the trend of Overall Technician Satisfaction from January 2, 2022 to March 25, 2022. The daily average score started at 4, rising sharply to 9.9 the next day and remaining above 9.5 for most of the period. Notable peaks occurred on January 8, March 12, 19, 20 and 25, where satisfaction reached 10. The score remained stable at approximately 9.8 between January 13 and March 11. 
# MAGIC
# MAGIC **Average Satisfaction by Country:**
# MAGIC
# MAGIC The regional analysis shows that Canada has the lowest average satisfaction rating of 9.4, followed by the USA, i.e., 9.75. The UK has the highest average satisfaction rating of 10.
# MAGIC
# MAGIC This suggests that additional focus is needed in Canada and the USA to improve satisfaction levels.
# MAGIC
# MAGIC **Issue Resolution Impact:**
# MAGIC
# MAGIC **Resolved Issues:** Median satisfaction score is 10, with most clients showing high satisfaction and some outliers.
# MAGIC
# MAGIC **Unresolved Issues:** Median satisfaction score is 4.5, with a broad range from 1 to 10 but no significant outliers.
# MAGIC
# MAGIC Resolved issues correlate with higher satisfaction scores, suggesting a need to improve resolution rates.
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("temp_view") # create a temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view 
# MAGIC LIMIT 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Case_Owner, Avg_Satis from
# MAGIC (Select Case_Owner,
# MAGIC Avg(Overall_Technician_Satisfaction) as Avg_Satis 
# MAGIC from temp_view 
# MAGIC Group By Case_Owner)
# MAGIC Order by Avg_Satis desc 
# MAGIC limit 5
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Case Owner Analysis:**
# MAGIC
# MAGIC The top 5 case owners all have an average satisfaction score of 10:
# MAGIC
# MAGIC - Cynthia Kristufek
# MAGIC - Sam Summerson
# MAGIC - Antonina Pemberton
# MAGIC - Johan Schafer
# MAGIC - Megan Patton
# MAGIC
# MAGIC This indicates consistent high performance among these technicians.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Recommendations:**
# MAGIC
# MAGIC
# MAGIC **1. Based upon Regional Analysis, focus on Regions with Lower Satisfaction**
# MAGIC
# MAGIC •	**Observation:** Canada (9.4) and the USA (9.75) have lower average satisfaction ratings compared to the UK (10)
# MAGIC
# MAGIC •	**Recommendation:** Investigate and address specific issues in Canada and the USA through surveys or interviews. Implement targeted initiatives such as additional training, resource allocation, mentoring and process improvements tailored to these regions.
# MAGIC
# MAGIC **2. Recognize and Reward Top-Performing Technicians**
# MAGIC
# MAGIC •	Establish a recognition and reward program for high-performing technicians with monthly or quarterly awards, bonuses, and public recognition, which will help maintain performance and also provide motivation to others. 
# MAGIC
# MAGIC **3. Provide Training and Mentorship to Technicians Handling Unresolved Issues based upon Issue Resolution Impact.**
# MAGIC
# MAGIC •	**Observation:** Cases with clients with unresolved issues by technicians have a median satisfaction score of 4.5, while those with resolved issues have a median satisfaction score of 10.
# MAGIC
# MAGIC **•	Recommendation:** Share best practices and success stories by implementing training and mentorship programs, where top-performing technicians mentor their peers to improve the overall satisfaction. 
# MAGIC
# MAGIC
