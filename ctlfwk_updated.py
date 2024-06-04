# Databricks notebook source
# MAGIC %md
# MAGIC #### ctlfwk notebook provides functionality to run a query against the control framework.
# MAGIC #### Returns a data frame of results.
# MAGIC March 2022

# COMMAND ----------

def ctlfwk_query(query):
    '''Returns a dataframe of results from the control framework sql database for a specified query'''
    ctlfwk_host = sql("SELECT value FROM ops.environment WHERE key = 'ctlfwk-host'").collect()[0][0]
    ctlfwk_db = sql("SELECT value FROM ops.environment WHERE key = 'ctlfwk-db'").collect()[0][0]
    
    url = "jdbc:sqlserver://{0};databaseName={1};".format(ctlfwk_host, ctlfwk_db)
    
    try:
        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("authentication", "ActiveDirectoryServicePrincipal") \
            .option("aadSecurePrincipalId", sql("select value from ops.environment where key = 'databricks-sp-id'").collect()[0][0]) \
            .option("aadSecurePrincipalSecret", dbutils.secrets.get(scope = "intelliversesecrets", key = "edp-common-databricks-operational-sp-secret")) \
            .option("encrypt", "true") \
            .option("hostNameInCertificate", "*.database.windows.net") \
            .load()
    except Exception as e:
        raise Exception("""Error executing the following query on '{0}' control framework. {1}""".format(ctlfwk_db, str(e)),query)
