# Databricks notebook source
# metadata/reader.py

def load_metadata(spark, jdbc_url, driver, table_name):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", driver) \
        .load()

def get_source_info(df, id_value, id_column="sourceid"):
    filtered = df.filter(f"{id_column} = '{id_value}'").collect()
    if not filtered:
        raise ValueError(f"[ERROR] No se encontró ningún registro con {id_column} = '{id_value}'")
    return filtered[0]


def get_asset_list(df_assets, sourceid):
    return df_assets.filter(f"sourceid = '{sourceid}'").collect()
