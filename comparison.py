# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Config

# COMMAND ----------

# MAGIC %run ./env

# COMMAND ----------

base_path = env["workspace_path"] + "/ncdr/data/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Sources

# COMMAND ----------

df_ecds = (spark.read
    .option("header", "true")
    .option("recursiveFileLookup", "True")
    .parquet(env["ecds_blob_path"])
    .filter(F.col("deleted") == 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

def create_spark_df_from_csv(base_path, file_name, delimeter=","):
    
    df = pd.read_csv(
        base_path + file_name,
        sep=delimeter
    )

    df = spark.createDataFrame(df)

    return df


def get_counts_within_date_range(df, col, start_date, end_date):
    df = (df
        .filter(F.col(col) > start_date)
        .filter(F.col(col) < end_date)
        .groupBy(col)
        .count()
        .orderBy(col)
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Col types

# COMMAND ----------

df_ecds_udal_col_types = (spark
    .createDataFrame(df_ecds.dtypes, ["column_name", "udal_data_type"])
)

df_ecds_ncdr_col_types = (
    create_spark_df_from_csv(
        base_path,
        'col_types.csv',
        '\\t'
    )
    .replace('varchar', 'string')                         
    .replace('datetime', 'timestamp')
    .replace('float', 'double')
    .replace('numeric', 'decimal(18,0)')
)

df_col_types_differences = (df_ecds_udal_col_types
    .join(df_ecds_ncdr_col_types, 'column_name', 'left')
    .filter(F.col('udal_data_type') != F.col('NCDR_DATA_TYPE'))         
)

display(df_col_types_differences)

# COMMAND ----------

col_type_differences = [row.column_name for row in df_col_types_differences.collect()]

display(df_ecds
    .select(*col_type_differences)
    .limit(100)        
)

# COMMAND ----------

# MAGIC %md
# MAGIC The only data type differences are some date/timestamp differences, but the timestamps are all at midnight.

# COMMAND ----------

# MAGIC %md
# MAGIC # Row count

# COMMAND ----------

df_ecds.count()

# COMMAND ----------

# MAGIC %md
# MAGIC row count as at 07/08/2024 12:40:
# MAGIC
# MAGIC - UDAL: 138,489,676
# MAGIC - NCDR: 138,592,338
# MAGIC
# MAGIC row count as at 07/08/2024 15:43:
# MAGIC
# MAGIC - UDAL: 138,592,335
# MAGIC - NCDR: 138,592,338
# MAGIC
# MAGIC row count as at 08/08/2024 11:35:
# MAGIC
# MAGIC - UDAL: 138,592,338
# MAGIC - NCDR: 138,682,590

# COMMAND ----------

# MAGIC %md
# MAGIC # Min and Max Dates

# COMMAND ----------

date_cols = [
  "Interchange_Received_Date",
  "CDS_Extract_Date",
  "Report_Period_Start_Date",
  "Report_Period_End_Date",
  "CDS_Applicable_Date",
  "CDS_Activity_Date",
  "Query_Date",
  "RTT_Period_Start_Date",
  "RTT_Period_End_Date",
  "Arrival_Date",
  "EC_Initial_Assessment_Date",
  "EC_Seen_For_Treatment_Date",
  "EC_Conclusion_Date",
  "EC_Departure_Date",
  "EC_Decision_To_Admit_Date",
  "EC_Injury_Date",
  "Der_EC_Arrival_Date_Time",
  "Der_EC_Departure_Date_Time",
  "Clinically_Ready_To_Proceed_Timestamp",
]

col_names = []
for col in date_cols:
  col_names.append('min_' + col)
  col_names.append('max_' + col)

date_cols_min = [F.min(col).alias('min_' + col) for col in date_cols]
date_cols_max = [F.max(col).alias('max_' + col) for col in date_cols]
date_cols_min_max = date_cols_min + date_cols_max

df_ecds_dates = (df_ecds
  .select(
    F.lit('udal').alias('source'),
    *date_cols_min_max
  )
  .unpivot(
    'source',
    col_names,
    'col_name',
    'udal_date'
  )          
)

df_ecds_ncdr_col_dates = create_spark_df_from_csv(
    base_path,
    'col_dates.csv',
    '\\t'
)

df_date_comparison = (df_ecds_dates
    .join(df_ecds_ncdr_col_dates, "col_name", "left")   
    .drop("source")                    
)

display(df_date_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC # Counts by date col comparisons

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interchange received date

# COMMAND ----------

df_ecds_ncdr_aug_ird = create_spark_df_from_csv(
    base_path,
    'aug_counts_ncdr.csv',
    '\\t'
)

df_ecds_udal_aug_ird = get_counts_within_date_range(
    df_ecds,
    'interchange_received_date',
    '2024-07-31',
    '2024-09-01'
)

display(df_ecds_udal_aug_ird
    .join(df_ecds_ncdr_aug_ird, 'interchange_received_date', 'left')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## report period start date

# COMMAND ----------

df_ecds_udal_rpsd = get_counts_within_date_range(
    df_ecds,
    'report_period_start_date',
    '2024-06-30',
    '2024-09-01'
)

df_ecds_ncdr_rpsd = create_spark_df_from_csv(
    base_path,
    'jul_aug_counts_ncdr_rpsd.csv',
    '\\t'
)

display(df_ecds_udal_rpsd
    .join(df_ecds_ncdr_rpsd, 'report_period_start_date', 'left')
)

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
