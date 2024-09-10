# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson Objectives
# MAGIC 1. Spark SQL documentation
# MAGIC 2. Create Database demo
# MAGIC 3. Data tab in the UI
# MAGIC 4. SHOW command
# MAGIC 5. DESCRIBE command
# MAGIC 6. Find the current database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED demo; 

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN default;

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning objectives
# MAGIC 1. Create managed table using Python
# MAGIC 2. Create managed tables using SQL
# MAGIC 3. Effect of dropping a managed table
# MAGIC 4. Describe table

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED race_results_python

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM demo.race_results_python
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_sql AS
# MAGIC SELECT * 
# MAGIC FROM demo.race_results_python
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning Objectives
# MAGIC 1. Create external table using python
# MAGIC 2. Create external table using SQL
# MAGIC 3. Effect of dropping an external table

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_ext_sql (
# MAGIC   race_year INT,
# MAGIC   race_name STRING,
# MAGIC   race_date TIMESTAMP,
# MAGIC   circuit_location STRING,
# MAGIC   driver_name STRING,
# MAGIC   driver_number INT,
# MAGIC   driver_nationality STRING,
# MAGIC   team STRING,
# MAGIC   grid INT,
# MAGIC   fastest_lap INT,
# MAGIC   race_time STRING,
# MAGIC   points FLOAT,
# MAGIC   position INT,
# MAGIC   created_date TIMESTAMP
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/formula1dljjcc/presentation/race_results_ext_sql'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo.race_results_ext_sql
# MAGIC SELECT * FROM demo.race_results_ext_py
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM demo.race_results_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %md
# MAGIC # Views on tables
# MAGIC ## Learning Objectives
# MAGIC 1. Create Temp View
# MAGIC 2. Create Global Temp View
# MAGIC 3. Create Permanent View 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_race_results AS
# MAGIC SELECT * FROM demo.race_results_python
# MAGIC WHERE race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS
# MAGIC SELECT * FROM demo.race_results_python
# MAGIC WHERE race_year = 2012;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW demo.pv_race_results AS
# MAGIC SELECT * FROM demo.race_results_python
# MAGIC WHERE race_year = 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;
# MAGIC SHOW TABLES;

# COMMAND ----------


