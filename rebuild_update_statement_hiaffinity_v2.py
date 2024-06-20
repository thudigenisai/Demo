# Databricks notebook source
# MAGIC %md
# MAGIC # Design Documentations
# MAGIC
# MAGIC Please read the design documents: 
# MAGIC https://jira.sew.com.au/wiki/pages/viewpage.action?pageId=169116461

# COMMAND ----------

import pandas as pd

# COMMAND ----------

def file_exists(dir):
  try:
    dbutils.fs.ls(dir)
  except:
    return False  
  return True

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
from pyspark.sql import Row
import re

def get_pii_columns_for_object_as_list(is_first_load, object_name):
    # Get PII columns for a given object
    # object_name:string -> source_object_attribute_name:list
    if is_first_load is False and spark._jsparkSession.catalog().tableExists('sew_slv_hiaffinity', object_name):
        q = "SELECT source_object_attribute_name FROM ctlfwk.vw_source_objects_attributes where Column_IS_PII = 'Y' and source_object_name = '{}' ".format(object_name)
        return ctlfwk_query(q).select('source_object_attribute_name').rdd.flatMap(lambda x: x).collect()
    else:
        return []
    
def get_pii_columns_for_object_as_list_det(is_first_load, object_name):
    # Get DET PII columns for a given object
    # object_name:string -> source_object_attribute_name:list
    if is_first_load is False and spark._jsparkSession.catalog().tableExists('sew_slv_hiaffinity', object_name):
        q = "SELECT source_object_attribute_name FROM ctlfwk.vw_source_objects_attributes where Column_IS_PII = 'Y' and Encryption_Type = 'DET' and source_object_name = '{}' ".format(object_name)
        return ctlfwk_query(q).select('source_object_attribute_name').rdd.flatMap(lambda x: x).collect()
    else:
        return []

def get_pii_columns_for_object_as_list_ndet(is_first_load, object_name):
    # Get NDET PII columns for a given object
    # object_name:string -> source_object_attribute_name:list
    if is_first_load is False and spark._jsparkSession.catalog().tableExists('sew_slv_hiaffinity_piiview', object_name):
        q = "SELECT source_object_attribute_name FROM ctlfwk.vw_source_objects_attributes where Column_IS_PII = 'Y' and Encryption_Type = 'NDET' and source_object_name = '{}' ".format(object_name)
        return ctlfwk_query(q).select('source_object_attribute_name').rdd.flatMap(lambda x: x).collect()
    else:
        return []

def rebuild_update_statement(update_statement_list_generator, root_records_pd, object_attributes, cdc_control_column_names):
    # Convert the changedatetime column from string to datetime format
    root_records_pd['changedatetime'] = pd.to_datetime(root_records_pd['changedatetime'], format="%Y-%m-%d %H:%M:%S.%f")

    result_spark_format = [] # Final result as list. It will be collected by the Spark job. All fields should have it's tpye as String
    result_pd_format = root_records_pd.head(1).copy()[0:0] # A duplicate result_spark_format list (staging) as Pandas df format to achieve better performance. Note: changedatetime should have it's type as pd._libs.tslibs.timestamps.Timestamp
    update_statement_list = list(update_statement_list_generator) # Get all update statements as list
    update_statement_list.sort(key=lambda row: pd.to_datetime(row["changedatetime"]), reverse=False) # Convert the changedatetime column to datetime format and sort update statement list by changedatetime column

    # Start rebuilding
    for update_statement in update_statement_list:# Iterate each raw update statement 
        recid = update_statement["_recid"]

        # If the "_Recid" of current row can be found in the "result_pd_format", use the current row and the latest row with same "_Recid" from "result_pd_format" to fill-up the miss columns. The latest row from "result_pd_format" can be defined as the row with the greatest value of "ChangeDateTime" column.
        # If the "_Recid" of current row CANNOT be found in the "result_pd_format", then find the a record from "root_records_pd" with same "_Recid", use the current row and the latest row with same "_Recid" from "root_records_pd" to fill-up the miss columns. The latest row from "root_records_pd" can be defined as the row with greatest value of "ChangeDateTime"
        historical_root_row = result_pd_format[result_pd_format["_recid"]==recid].sort_values(by='changedatetime', ascending=False).head(1)
        if historical_root_row.empty:
            historical_root_row = root_records_pd[root_records_pd["_recid"]==recid].sort_values(by='changedatetime', ascending=False).head(1)
        # Do not rebuild the record if no root record can be found
        if historical_root_row.empty:
            result_spark_format.append(update_statement)
            continue
        
        # Start rebuilding the fields
        changed_fields = update_statement["changed_fields"].split(";") # Get all changed fields
        new_row_pd = {}
        new_row_spark = {}
        for field_name in update_statement.__fields__: # Start rebuilding every fields for the raw update statement
            org_row_value = update_statement[field_name] # Get raw field value
            if field_name in changed_fields or field_name.lower() in cdc_control_column_names or org_row_value is not None:
                field_value = org_row_value # Use the raw value here if the field has been marked as changed field or it's control column or it has value
            else:
                field_value = historical_root_row.iloc[0][field_name] # Get field vaule from historical record
                # Special treatment for dataype "date" (Progress DB), adding tailing " 00:00:00.0000000" 
                if object_attributes[field_name.lower()] == "date" and field_value is not None and re.match(r'^\d{4}-\d{2}-\d{2}$', field_value):
                    field_value = str(field_value) + " 00:00:00.0000000"
            # If it's final result in the staging Pandas dataframe, then the type for changedatetime column will be datetime
            new_row_pd[field_name] = pd.to_datetime(field_value, format="%Y-%m-%d %H:%M:%S.%f") if field_name=="changedatetime" and isinstance(field_value, str) else field_value
            # If it's final result, then the type for changedatetime column will be string
            new_row_spark[field_name] = field_value.strftime("%Y-%m-%d %H:%M:%S.%f") if field_name=="changedatetime" and isinstance(field_value, pd._libs.tslibs.timestamps.Timestamp) else field_value
        new_row_pd_df = pd.DataFrame([new_row_pd])
        result_pd_format = pd.concat([result_pd_format,new_row_pd_df], ignore_index=True) # Insert into the staging final result dataframe, it will be used for next iteration
        result_spark_format.append(new_row_spark) # Insert into the final result list and it will be collected by the Spark job.

        # Debug only, showing the raw update statements
        #result_spark_format.append(update_statement)
    
    return iter(result_spark_format)


def handle_rebuild(is_first_load, source_app, object_name, csv_table_name, cdc_datafile_list, storage_account, op):

    # Get all columns from ctlfwk. The list will be used for selecting final result.
    org_column_ordering = []
    for row in ctlfwk_query("select source_object_attribute_name, source_object_attribute_seq from ctlfwk.vw_source_objects_attributes where source_object_name = '{}'".format(object_name)).orderBy("source_object_attribute_seq").collect():
        org_column_ordering.append(row["source_object_attribute_name"])

    # CDC control columns
    cdc_control_column_names = ["_Recid","changed_fields","_Tran-Id","_Tran-id","_Time-Stamp","_Change-Sequence","_Continuation-Position","_Array-Index","_Fragment", "_Operation", "_ArrayIndex"]

    # Get pii columns from ctlfwk
    pii_column_names = get_pii_columns_for_object_as_list(is_first_load, object_name)
    pii_column_names_det = get_pii_columns_for_object_as_list_det(is_first_load, object_name)
    pii_column_names_ndet = get_pii_columns_for_object_as_list_ndet(is_first_load, object_name)

    # Get column name - type from ctlfwk as map
    object_attributes = {}
    for row in ctlfwk_query("select source_object_attribute_name, source_object_attribute_data_type from ctlfwk.vw_source_objects_attributes where source_object_name = '{}' ".format(object_name)).collect():
        object_attributes[row["source_object_attribute_name"]] = row["source_object_attribute_data_type"]

    # Make case-insensitive for all column names
    object_attributes = {k.lower(): v for k, v in object_attributes.items()}
    cdc_control_column_names = {v.lower() for v in cdc_control_column_names}
    pii_column_names = {v.lower() for v in pii_column_names}
    pii_column_names_det = {v.lower() for v in pii_column_names_det}
    pii_column_names_ndet = {v.lower() for v in pii_column_names_ndet}

    # Convert column names to select string
    col_sql = "".join(["`" + attributes_name + "`," for attributes_name in list(object_attributes.keys())]).rstrip(',')
    col_sql_pii = (col_sql + "," + "".join(["`" + attributes_name + "_pii`," for attributes_name in pii_column_names])).rstrip(',')
    col_sql_org_column_ordering = "".join(["`" + attributes_name + "`," for attributes_name in org_column_ordering]).rstrip(',') # Only use for selecting final result
    
    # Select all insert statements from csv file(s)
    insert_df = spark.sql("""
    SELECT {0} FROM {1} WHERE `_Operation`=1
    """.format(col_sql, csv_table_name)).alias('cdc_insert')

    # Select all delete statements from csv file(s)
    delete_df = spark.sql("""
    SELECT {0} FROM {1} WHERE `_Operation`=2
    """.format(col_sql, csv_table_name))

    # Select all update statements from csv file(s)
    update_df = spark.sql("""
    SELECT {0} FROM {1}  WHERE `_Operation`=4 ORDER BY cast(`_Change-Sequence` AS BIGINT) ASC;
    """.format(col_sql, csv_table_name)).alias('cdc_update')

    # Log the counts by operation type
    op.info("Insert statements count for object {}".format(object_name), extra={'Insert statements count':insert_df.count()})
    op.info("Update statements count for object {}".format(object_name), extra={'Update statements count':update_df.count()})
    op.info("Delete statements count for object {}".format(object_name), extra={'Delete statements count':delete_df.count()})

    # All the insert statements from CDC csv files will be the history root records
    root_records_df = insert_df.join(update_df,insert_df["_recid"] == update_df["_recid"],"inner").select("cdc_insert.*").select(col_sql.split(","))
    # Finding the history root records from silver table if it's not a first load
    if is_first_load is False and spark._jsparkSession.catalog().tableExists('sew_slv_hiaffinity', object_name): # Get the decrypted values from piiview table
        # Inner join the "update_df" and the "silver_table" (join key: "_Recid"), select the required columns from "silver_table" and create a table to store the results: "history_root_table". Please note: the records from "silver_table" for each "_Recid" should have the greatest value of "ChangeDateTime" column
        slv_df = spark.sql("SELECT {} ,ROW_NUMBER() OVER(PARTITION BY `_recid` ORDER BY `ChangeDateTime_Aet` DESC) AS `ROW_NUMBER` FROM sew_slv_hiaffinity.{}".format(col_sql, object_name)).filter(col("ROW_NUMBER")==1).drop("ROW_NUMBER").alias('slv')# Only select the latest record for given _recid
        root_slv_df = slv_df.join(update_df,slv_df["_recid"] == update_df["_recid"],"inner").select("slv.*") # Do the inner join and only select the columns from silver table
        root_slv_df.createOrReplaceTempView("root_slv_df_tmp")
        pii_col_query_list = []
        if len(pii_column_names_det) > 0:
            for pii_column_name_det in pii_column_names_det:
                pii_col_query_list.append(", decrypt_scala_det_binary(`{0}`) as `{0}_pii`".format(pii_column_name_det))
        if len(pii_column_names_ndet) > 0:
            for pii_column_name_ndet in pii_column_names_ndet:
                pii_col_query_list.append(", decrypt_scala_ndet_binary(`{0}`) as `{0}_pii`".format(pii_column_name_ndet))
        select_pii = "".join(q for q in pii_col_query_list)
        root_slv_df = spark.sql("select * {0} from root_slv_df_tmp".format(select_pii))
        # Drop encrypted columns and rename _pii columns to it's original names. 
        for pii_column_name in pii_column_names:      
            root_slv_df = root_slv_df.drop(pii_column_name).withColumnRenamed(pii_column_name+"_pii", pii_column_name)
        # Convert all values to String
        root_slv_df = root_slv_df.select([col(c).cast("string") for c in root_slv_df.columns]).select(col_sql.split(","))
        # Adding silver root records and CDC insert statements together as root records
        root_records_df = root_slv_df.unionAll(root_records_df)
        op.info("Root records from slv table count:{}".format(object_name), extra={'Root records count':root_slv_df.count()})
    
    # Get the root records as Pandas dataframe. It will be used for as a broadcast variable
    root_records_df_pd = root_records_df.toPandas().drop_duplicates(subset=['_recid'])
    op.info("Root records in total:{}".format(object_name), extra={'Root records in total count':root_records_df_pd.shape[0]})

    # Force the minimum job parallelism level to 10
    number_of_partition = 10 if update_df.rdd.getNumPartitions() < 10 else update_df.rdd.getNumPartitions()
    update_df = update_df.repartition(number_of_partition, "_recid")
    op.info("Update statement df number_of_partition:{}".format(object_name), extra={'Update statement df number_of_partition':update_df.rdd.getNumPartitions()})

    org_schema_update_df = update_df.schema
    # Start the Spark transformation (rebuilding update statements). 
    update_df_rdd = update_df.rdd.mapPartitions(lambda x: rebuild_update_statement(x, root_records_df_pd, object_attributes, cdc_control_column_names))
    update_df_rdd_count = update_df_rdd.count()
    op.info("Update statement df RDD count:{}".format(object_name), extra={'Update statement df RDD count':update_df_rdd_count})
    update_df = spark.createDataFrame(update_df_rdd, schema=org_schema_update_df)

    #Write final results as a csv file into a temp path (dbfs)
    ymd_datetime = datetime.utcnow().strftime("%Y%m%d")
    op.info("Start writing csv file... Object: {}".format(object_name), extra={'Start writing csv file':"Start writing csv file"})
    final_result_df = insert_df.unionAll(update_df).unionAll(delete_df)
    final_result_df = final_result_df.filter(final_result_df["_tran-id"].isNotNull()) #select only cdc rows
    
    op.info("Start writing final csv file. Object: {}".format(object_name), extra={'Start writing csv file':"target incoming"})
    write_to_path = "abfss://incoming@"+ storage_account+".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    spark.sql("DROP TABLE IF EXISTS sew_work.hiaffinity_temp_csv_{}".format(object_name))
    # Use delta format to get better performance
    final_result_df.write.mode('overwrite').format("delta").saveAsTable("sew_work.hiaffinity_temp_csv_{}".format(object_name))
    spark.sql("select {0} from sew_work.hiaffinity_temp_csv_{1}".format(col_sql_org_column_ordering, object_name)).coalesce(1).write.format('csv').mode('overwrite').option("header",True).option("ignoreNullFields", "false").save(write_to_path)
    

    op.info("Start file cleaning + restructuring. Object: {}".format(object_name), extra={'Start file cleaning + restructuring':"Start file cleaning + restructuring"})
    # cleaning + restructuring operations
    for f in dbutils.fs.ls(write_to_path):
        if (f.name).startswith('_'):
            dbutils.fs.rm(f.path, True)

    # renaming the new csv file
    incoming_csv_files = []
    for f in dbutils.fs.ls(write_to_path):
        old_name = write_to_path + "/" + f.name
        new_name = write_to_path + "_" + object_name + "_cdc.csv"
        dbutils.fs.mv(old_name, new_name) # moving a file from child folder to a parent folder
        incoming_csv_files.append(new_name)

    # removing the empty child directory
    delete_from_path = write_to_path+"/"
    dbutils.fs.rm(delete_from_path)
    
    #Move original cdc csv file(s) into archive
    op.info("Start file movement to archive. Object: {}".format(object_name), extra={'Start file movement to archive':"Start file movement to archive"})
    archive_path = "abfss://archive@" + storage_account + ".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + ymd_datetime + "_" + object_name + "_" + "before_rebuild"
    dbutils.fs.mkdirs(archive_path)
    for filename in cdc_datafile_list:
        current_file_path = "abfss://incoming@"+ storage_account+".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + filename
        dbutils.fs.mv(current_file_path, archive_path,recurse=True) # moving the cdc file to the archive path

    return root_records_df_pd, update_df, incoming_csv_files
