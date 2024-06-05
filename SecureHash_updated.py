# Databricks notebook source
# MAGIC %md
# MAGIC # SecureHash
# MAGIC ### This notebook contains the scala UDF function to return the SHA2-256 hash of the supplied columns including the secret pepper.
# MAGIC #### February 2022
# MAGIC Key points:
# MAGIC * This notebook is called by the silver and gold wrapper notebooks.
# MAGIC * The 'hash_secure' function is called in the Jinja2 templates where hash generation is required.

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Base64
# MAGIC import org.apache.spark.sql.functions.{udf, array, lit}
# MAGIC import org.apache.spark.sql.functions.unbase64

# COMMAND ----------

# MAGIC %scala
# MAGIC lazy val sha = java.lang.ThreadLocal.withInitial(() =>java.security.MessageDigest.getInstance("SHA-256"))
# MAGIC
# MAGIC val business_unit_name_code = dbutils.widgets.get("business_unit_name_code")
# MAGIC val scope_name = "acumen-key-vault-scope"
# MAGIC val key_name =  "adb-hash-pepper"
# MAGIC
# MAGIC // Retrieve pepper from key vault
# MAGIC val pepper = Base64.getDecoder().decode(dbutils.secrets.get(scope = scope_name, key = key_name))
# MAGIC //val pepper = Base64.getDecoder().decode(key_name) 
# MAGIC // Define UDF function, read in sequence of values, append with pepper then return SHA2 hash
# MAGIC spark.udf.register(
# MAGIC   "secure_hash",
# MAGIC   (cols: Seq[String], sep: String) => {
# MAGIC     sha.get().digest(cols.mkString(sep).getBytes() ++ pepper)
# MAGIC   }
# MAGIC )

# COMMAND ----------


