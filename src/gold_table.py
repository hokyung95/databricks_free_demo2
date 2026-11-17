# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC
# MAGIC %sql
# MAGIC select 
# MAGIC   Description, 
# MAGIC   sum(Quantity) as sum_quantity, 
# MAGIC   round(avg(UnitPrice), 2) as avg_unit_price,
# MAGIC   round(sum(TotalPrice), 2) as total_price 
# MAGIC from lsm_test.`lsmhk-external-schema`.online_retail_silver
# MAGIC where InvoiceDate >= '2010-12-01'
# MAGIC group by Description

# COMMAND ----------

# DBTITLE 1,Silver table의 Data를 집계하여 Spark Dataframe 생성
query = """
    select 
        Description, 
        sum(Quantity) as sum_quantity, 
        round(avg(UnitPrice), 2) as avg_unit_price,
        round(sum(TotalPrice), 2) as total_price
    from lsm_test.`lsmhk-external-schema`.online_retail_silver 
    where InvoiceDate >= '2010-12-01'
    group by Description
"""

df = spark.sql(query)

# COMMAND ----------

from pyspark.sql import functions as F

df = (spark.table("lsm_test.`lsmhk-external-schema`.online_retail_silver")
    .filter(F.col("InvoiceDate") >= "2010-12-01")
    .groupBy("Description")
    .agg(
        F.sum("Quantity").alias("sum_quantity"),
        F.round(F.avg("UnitPrice"), 2).alias("avg_unit_price"),
        F.round(F.sum("TotalPrice"), 2).alias("total_price")
    ))

# COMMAND ----------

# DBTITLE 1,집계한 데이터 건수 확인
df.count()

# COMMAND ----------

# DBTITLE 1,Gold table 생성
df.write.mode("overwrite").saveAsTable("lsm_test.`lsmhk-external-schema`.online_retail_gold")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,데이터 건수 확인
# MAGIC %sql
# MAGIC select count(*) from lsm_test.`lsmhk-external-schema`.online_retail_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lsm_test.`lsmhk-external-schema`.online_retail_gold;