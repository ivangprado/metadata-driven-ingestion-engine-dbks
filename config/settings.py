# Databricks notebook source
# Retrieve the storage account key securely from Databricks Secret Scope
storage_key = dbutils.secrets.get(scope="myscope", key="storage-key")

# Configure Spark to access Azure Data Lake securely
spark.conf.set(
    "fs.azure.account.key.myigpdatalake.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------

# Retrieve SQL Server credentials securely from Secret Scope
sql_user = dbutils.secrets.get(scope="myscope", key="sql_user")
sql_password = dbutils.secrets.get(scope="myscope", key="sql_password")

# Build the JDBC URL securely using the secrets
JDBC_URL = f"jdbc:sqlserver://igpserver.database.windows.net:1433;database=metadata;user={sql_user};password={sql_password}"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

RAW_BASE_PATH = "abfss://raw@myigpdatalake.dfs.core.windows.net"
DATAHUB_BASE_PATH = "abfss://datahub@myigpdatalake.dfs.core.windows.net"
