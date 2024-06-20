# Databricks notebook source
# MAGIC %run /dp-databricks-data-patterns/lib/ctlfwk

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import *

# NEW HANDLE PII FUNCTION
def handle_pii(object_name, recid, attribute_name): # More params here
    q = 'select `{}_pii` from sew_slv_hiaffinity_piiview.{} where _Recid = {} '.format(attribute_name, object_name, recid)
    return spark.sql(q).select(attribute_name+'_pii').collect()[0][0]

def get_pii_columns_for_object_as_list(is_first_load, object_name):
    # Get PII columns for a given object
    # object_name:string -> source_object_attribute_name:list
    if is_first_load is False and spark._jsparkSession.catalog().tableExists('sew_slv_hiaffinity', object_name):
        q = "SELECT source_object_attribute_name FROM ctlfwk.vw_source_objects_attributes where Column_IS_PII = 'Y' and source_object_name = '{}' ".format(object_name)
        return ctlfwk_query(q).select('source_object_attribute_name').rdd.flatMap(lambda x: x).collect()
    else:
        return []

# source csv file name: 2023_source.csv
# cdc csv file name: 2023_cdc.csv
def handle_rebuild(is_first_load, source_app, object_name, csv_table_name, cdc_datafile_list, storage_account):

    cdc_control_column_names = ["_Recid","changed_fields","_Tran-Id","_Time-Stamp","_Change-Sequence","_Continuation-Position","_Array-Index","_Fragment", "_Operation", "_ArrayIndex"]
    pii_column_names = get_pii_columns_for_object_as_list(is_first_load, object_name)
    object_attributes = {}
    for row in ctlfwk_query("select source_object_attribute_name, source_object_attribute_data_type from ctlfwk.vw_source_objects_attributes where source_object_name = '{}' ".format(object_name)).collect():
        object_attributes[row["source_object_attribute_name"]] = row["source_object_attribute_data_type"]
    
    #Select insert statements into the result table
    spark.sql("DROP TABLE IF EXISTS `hiaffinity_post_extraction_{}_result`".format(object_name))
    sql_result_df = """
    SELECT * FROM {} WHERE `_Operation`=1
    """.format(csv_table_name)
    result_df = spark.sql(sql_result_df)
    result_df.write.mode('overwrite').saveAsTable("`hiaffinity_post_extraction_{}_result`".format(object_name))

    #Select delete statements into the temp delete table
    spark.sql("DROP TABLE IF EXISTS `hiaffinity_post_extraction_{}_delete_statement`".format(object_name))
    sql_delete_df = """
    SELECT * FROM {} WHERE `_Operation`=2
    """.format(csv_table_name)
    delete_df = spark.sql(sql_delete_df)
    delete_df.write.mode('overwrite').saveAsTable("`hiaffinity_post_extraction_{}_delete_statement`".format(object_name))

    #Select update statements
    sql_load_update_df = """
    SELECT * FROM {}  WHERE `_Operation`=4 ORDER BY cast(`_Change-Sequence` AS BIGINT) ASC;
    """.format(csv_table_name)
    update_df = spark.sql(sql_load_update_df)

    for row in update_df.toLocalIterator(): #TODO, No local here        
        recid = row["_Recid"]
        historical_root_row = row
        root_row_source = "self"

        # Find the root row
        # Look up the result table first
        historical_root_row_csv_df = spark.sql("SELECT * FROM {} WHERE `_Recid`={} ORDER BY cast(`_Change-Sequence` AS BIGINT) DESC LIMIT 1;".format("`hiaffinity_post_extraction_{}_result`".format(object_name),recid))
        if historical_root_row_csv_df.count()>0:
            historical_root_row = historical_root_row_csv_df.collect()[0]
            root_row_source = "csv"
        elif spark._jsparkSession.catalog().tableExists('sew_slv_{}'.format(source_app), object_name): # Look up the silver table
            historical_root_row_slv_df = spark.sql("SELECT * FROM {}.{} WHERE `_Recid`={} ORDER BY `ChangeDateTime_Aet` DESC LIMIT 1;".format('sew_slv_{}'.format(source_app), object_name,recid))
            if historical_root_row_slv_df.count()>0:
                historical_root_row = historical_root_row_slv_df.collect()[0]
                root_row_source = "slv"
            else:
                break #If not root row found, do not ingest the current update statement
        else:
            break #If not root row found, do not ingest the current update statement

        #Rebuild the fields
        changed_fields = row["changed_fields"].split(";")
        new_row_values = ""
        for field_name in row.__fields__:
            org_row_value = row[field_name]
            if field_name in changed_fields or field_name in cdc_control_column_names or org_row_value is not None:
                field_value = org_row_value
            else:
                if field_name in pii_column_names and root_row_source == "slv":
                    field_value = handle_pii(object_name,recid,field_name)
                else:
                    field_value = historical_root_row[field_name]

                if root_row_source == "slv":
                    field_data_type = object_attributes[field_name]
                    if 'date' in field_data_type and field_value is not None:
                        field_value = str(field_value) + " 00:00:00.0000000"
                
            field_value = "null," if field_value is None else "'" + str(field_value) + "',"
            new_row_values = new_row_values + field_value

        new_row_values = new_row_values.rstrip(',')
        new_row_values_insert = "INSERT INTO `hiaffinity_post_extraction_{}_result` VALUES ({});".format(object_name, new_row_values)
        spark.sql(new_row_values_insert)

    #Union result table and delete table
    union_result_delete_sql = """
    INSERT INTO `hiaffinity_post_extraction_{0}_result` TABLE `hiaffinity_post_extraction_{0}_delete_statement`
    """.format(object_name)
    spark.sql(union_result_delete_sql)


    ymd_datetime = datetime.utcnow().strftime("%Y%m%d")
    make_dir_path = "abfss://archive@" + storage_account + ".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + ymd_datetime +"_"+object_name+ "_" + "before_rebuild"
    dbutils.fs.mkdirs(make_dir_path)

    for filename in cdc_datafile_list:
        # move all  files from the cdc_datafile_list into
        current_file_path = "abfss://incoming@"+ storage_account+".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + filename
        move_file_path = "abfss://archive@"+ storage_account+".dfs.core.windows.net/" + source_app +'/' + object_name + '/' + ymd_datetime +"_"+object_name+"_"+"before_rebuild"

        dbutils.fs.mv(current_file_path, move_file_path,recurse=True) # moving the cdc file to the archive path

    # write the dataframe as csv into the main incoming dir of the object
    cdc_data_result_table = spark.sql("Select * From hiaffinity_post_extraction_{0}_result Where `_Tran-id` is not null and `_Change-Sequence` is not null".format(object_name))

    # logic to write the cdc_data_result_table dataframe as a csv
    write_to_path = "abfss://incoming@"+ storage_account+".dfs.core.windows.net/" + source_app + '/' + object_name + '/' + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    cdc_data_result_table.coalesce(1).write.format('csv').option("header",True).option("ignoreNullFields", "false").save(write_to_path)

    # cleaning + restructuring operations
    for file in dbutils.fs.ls(write_to_path):
        if (file.name).startswith('_'):
            print("Deleted: "+ file.name)
            dbutils.fs.rm(file.path, True)

    # renaming the new csv file
    for file in dbutils.fs.ls(write_to_path):
        old_name = write_to_path + "/" + file.name
        new_name = write_to_path + "_" + object_name + "_cdc.csv"
        dbutils.fs.mv(old_name, new_name) # moving a file from child folder to a parent folder
    
    # removing the empty child directory
    delete_from_path = delete_from_path = write_to_path+"/"
    if dbutils.fs.rm(delete_from_path) is True:
        print("Deleted the child directory: {}".format(delete_from_path))
