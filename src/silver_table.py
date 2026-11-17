# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
spark.table("lsm_test.`lsmhk-external-schema`.online_retail_bronze").show()

# COMMAND ----------

# DBTITLE 1,sql도 가능
# MAGIC %sql
# MAGIC select *, (Quantity * UnitPrice) as TotalPrice from 
# MAGIC lsm_test.default.online_retail_bronze 
# MAGIC where InvoiceNo not like 'C%'

# COMMAND ----------

# DBTITLE 1,Bronze table을 정재하는 쿼리를 Spark로 실행하여 Dataframe 생성

query = """
    select *, (Quantity * UnitPrice) as TotalPrice from 
    lsm_test.`lsmhk-external-schema`.online_retail_bronze
    where InvoiceNo not like 'C%'
"""

df = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Bronze table을 정재하는 Spark code로 Dataframe 생성
from pyspark.sql.functions import col

df = spark.table("lsm_test.`lsmhk-external-schema`.online_retail_bronze") \
    .filter(~col("InvoiceNo").like("C%")) \
    .withColumn("Quantity", col("Quantity").cast("int")) \
    .withColumn("UnitPrice", col("UnitPrice").cast("double")) \
    .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# COMMAND ----------

# DBTITLE 1,정재된 건수 확인
df.count()

# COMMAND ----------

# DBTITLE 1,Silver table 생성
df.write.mode("overwrite").saveAsTable("lsm_test.`lsmhk-external-schema`.online_retail_silver")

# COMMAND ----------

# DBTITLE 1,확인
# MAGIC %sql
# MAGIC select count(*) from lsm_test.`lsmhk-external-schema`.online_retail_silver