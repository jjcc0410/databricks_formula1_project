# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    """
    Adds an ingestion date column to the input DataFrame with the current timestamp.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame.

    Returns:
    DataFrame: A new DataFrame with an additional 'ingestion_date' column containing the current timestamp.
    """
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    """
    Re-arranges the columns of the input DataFrame so that the partition column is the last column.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame.
    partition_column (str): The column name to be moved to the last position.

    Returns:
    DataFrame: A new DataFrame with the partition column as the last column.
    """
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    """
    Overwrites partitions of a table with the input DataFrame. If the table does not exist, it creates a new partitioned table.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame to be written.
    db_name (str): The name of the database.
    table_name (str): The name of the table.
    partition_column (str): The column name on which the table is partitioned.

    Returns:
    None
    """
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    """
    Converts a distinct column of a Spark DataFrame to a list.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame.
    column_name (str): The name of the column to be converted to a list.

    Returns:
    list: A list of distinct values from the specified column.
    """
    df_row_list = input_df.select(column_name).distinct().collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    """
    Merges the input DataFrame into an existing Delta table or creates a new Delta table if it does not exist.

    Parameters:
    input_df (DataFrame): The input Spark DataFrame to be merged.
    db_name (str): The name of the database.
    table_name (str): The name of the table.
    folder_path (str): The folder path where the Delta table is stored.
    merge_condition (str): The condition on which to merge the data.
    partition_column (str): The column name on which the table is partitioned.

    Returns:
    None
    """
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning.enabled", "true")

    from delta.tables import DeltaTable

    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
