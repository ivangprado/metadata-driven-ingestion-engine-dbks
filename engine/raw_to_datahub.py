# Databricks notebook source
# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../metadata/reader

# COMMAND ----------

# MAGIC %run ../utils/scd

# COMMAND ----------

# MAGIC %run ../config/settings

# COMMAND ----------

# notebooks/raw_to_datahub.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date

spark = SparkSession.builder.getOrCreate()

log_info("Starting migration process from RAW to DataHub")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("assetname", "")
dbutils.widgets.text("execution_year", "")
dbutils.widgets.text("execution_month", "")
dbutils.widgets.text("execution_day", "")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
asset_name = dbutils.widgets.get("assetname")

execution_year = dbutils.widgets.get("execution_year")
execution_month = dbutils.widgets.get("execution_month")
execution_day = dbutils.widgets.get("execution_day")

log_info(f"Processing asset: {asset_name} (ID: {asset_id}) from source: {source_id}")
log_info(f"Execution date: {execution_year}-{execution_month.zfill(2)}-{execution_day.zfill(2)}")

# COMMAND ----------

# Read column metadata
log_info("Loading asset column metadata")
df_columns = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.assetcolumns")
schema = df_columns.filter(f"assetid = '{asset_id}'").select("columnname", "ispk").collect()
pk_cols = [row["columnname"] for row in schema if row["ispk"]]

log_info(f"PK columns identified: {pk_cols}")

# COMMAND ----------

# Paths
parquet_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_year={execution_year}/ingestion_month={execution_month}/ingestion_day={execution_day}/"
delta_path = f"{DATAHUB_BASE_PATH}/{source_id}/{asset_name}/"

log_info(f"Source path (RAW): {parquet_path}")
log_info(f"Target path (DataHub): {delta_path}")

# COMMAND ----------

# Read Parquet
log_info("Reading data from RAW layer (Parquet)")
try:
    df = spark.read.parquet(parquet_path)
    log_info(f"Data read successfully. Number of records: {df.count()}")
except Exception as e:
    log_error(f"Error reading parquet: {str(e)}")
    raise

# COMMAND ----------

# Load expected schema from metadata, ordered by columnid
schema = df_columns.filter(df_columns.assetid == asset_id).orderBy("columnid").collect()
final_column_names = [col["columnname"] for col in schema]

# Check if the number of columns matches
if len(final_column_names) != len(df.columns):
    log_error(f"Column count mismatch: expected {len(final_column_names)}, got {len(df.columns)}")
    raise Exception("[ERROR] Number of columns in data doesn't match metadata definition")

# Rename columns by position, using the metadata-defined names
df = df.toDF(*final_column_names)

log_info(f"Column renaming completed successfully: {final_column_names}")


# COMMAND ----------

# SCD2
if pk_cols:
    log_info("Applying SCD Type 2 treatment")
    df = df.dropDuplicates(pk_cols)
    log_info(f"Duplicates removed by PK. Remaining records: {df.count()}")

    df= prepare_scd2_columns(df, execution_year, execution_month, execution_day)
    log_info("SCD2 columns added to dataframe")

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, delta_path):
        log_info("Delta table already exists. Executing SCD2 merge")
        apply_scd2_merge(spark, df, delta_path, pk_cols)
        log_info("SCD2 merge completed successfully")
    else:
        log_info("Delta table doesn't exist. Creating new table")
        df.write.format("delta") \
            .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
            .mode("overwrite").save(delta_path)
        log_info("Delta table created successfully")
else:
    log_warning("No PK columns found. Performing full overwrite")
    df.write.format("delta") \
            .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
            .mode("overwrite").save(delta_path)
    log_info("Data written successfully in Delta format")

# COMMAND ----------

# %sql
# SELECT * 
# FROM delta.`abfss://datahub@myigpdatalake.dfs.core.windows.net/S_SQL/clientes`
# WHERE Email IS NOT NULL;
