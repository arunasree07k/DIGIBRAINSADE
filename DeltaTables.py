# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sales_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", IntegerType(), True),
    StructField("Discount", IntegerType(), True),
    StructField("OrderDate", StringType(), True)
])

# COMMAND ----------

sales_df=spark.read.format('csv').option('header',True).schema(sales_schema).load('/Volumes/digibrainsade/pyspark/ade/Sales.csv')

# COMMAND ----------

sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction to Delta Tables
# MAGIC ####Creating Delta Tables
# MAGIC

# COMMAND ----------

spark.sql('create database if not exists digidb')

# COMMAND ----------

spark.sql('describe schema digidb').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema digidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC use digidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

sales_df.write.format('delta').mode('overwrite').option('overwriteSchema',True).saveAsTable('digidb.sales')

# COMMAND ----------

spark.sql('select * from sales').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists sales29 (
# MAGIC     OrderID INT,
# MAGIC     CustomerName VARCHAR(100),
# MAGIC     City VARCHAR(50),
# MAGIC     Product VARCHAR(100),
# MAGIC     Category VARCHAR(50),
# MAGIC     Quantity INT,
# MAGIC     UnitPrice DECIMAL(10,2),
# MAGIC     Discount DECIMAL(5,2),
# MAGIC     OrderDate DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales29;

# COMMAND ----------

sales_df.write.format('delta').mode('append').saveAsTable('digidb.sales29')

# COMMAND ----------

data=[(1016,'Rohit Sharma','Mumbai','Laptop','Electronics',1,50000,8.00,'2026-10-01')]
schema=('OrderID','CustomerName','City','Product','Category','Quantity','UnitPrice','Discount','OrderDate')
new_df=spark.createDataFrame(data,schema)
new_df.display()

# COMMAND ----------

new_df.write.format('delta').mode('append').saveAsTable('digidb.sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales;

# COMMAND ----------

from delta.tables import DeltaTable
delta_sales=DeltaTable.forName(spark,'digidb.sales')
#delta_sales.update(condition="OrderID=1004",set={'City':"'Delhi'"})

# COMMAND ----------

delta_sales.delete(condition="OrderID=1003")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales values (1017, 'John Doe', 'New York', 'Laptop', 'Electronics', 1, 1000, 0.1, '2022-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC update sales set Quantity=8 where OrderID=1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sales where OrderID=1008;

# COMMAND ----------

delta_sales.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from digidb.sales version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from digidb.sales timestamp as of '2026-10-01T15:59:18.000+00:00'

# COMMAND ----------

ver_df=spark.read.format('delta').option('versionAsof',6).table('digidb.sales')

# COMMAND ----------

ver_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table digidb.sales to version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum digidb.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum digidb.sales retain 2400 hours;