# Databricks notebook source
# MAGIC %md
# MAGIC # wip_to_bronze_append
# MAGIC ### This notebook moves data from the wip container to the bronze container.
# MAGIC #### November 2021
# MAGIC Key points:
# MAGIC * This notebook is called by ADF and ADF supplies the input parameters including the table schema information in a JSON format.
# MAGIC * Jinja2 template is rendered using the table schema and input parameters. This produces a set of SQL queries.
# MAGIC * SQL queries are executed to process the data from wip to bronze. Multiple files for the same table can be read from wip and processed.
# MAGIC * Bronze table includes all source columns plus some "bronze" audit columns such as Source_Date and Source_Filename.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import data quality (GE) functionality

# COMMAND ----------

# MAGIC %run ../utilities/data_quality_validator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Logging functionality with Application Insights

# COMMAND ----------

# MAGIC %run deploy/dp-databricks-logging-py/Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import bronze Jinja2 templates

# COMMAND ----------

# MAGIC %run ../patterns/brz_load_start

# COMMAND ----------

# MAGIC %run ../patterns/brz_load_end

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import classes for pattern execution

# COMMAND ----------

# MAGIC %run ../lib/layer_inputs

# COMMAND ----------

# MAGIC %run ../lib/template_executor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import ADLS configuration function

# COMMAND ----------

# MAGIC %run ../lib/adls_config_executor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import table metrics function

# COMMAND ----------

# MAGIC %run ../lib/get_table_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import drop worker tables function

# COMMAND ----------

# MAGIC %run ../lib/drop_worker_tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import get_formatted_time_aet function

# COMMAND ----------

# MAGIC %run ../lib/time

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Import the required libraries

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone
import json
from jinja2 import Template
from pyspark.sql import SQLContext
import sys, os
from pyspark.sql.functions import udf, lit, md5, col
from pyspark.sql.types import StringType,StructType
from delta.tables import *

# COMMAND ----------

def get_tpl_params(exe_obj):
  '''Create a python dictionary that will be used in addition to the schema dictionary to render Jinja2 templates'''
  tpl_params = {
    "incoming_path" :exe_obj.incoming_path,
    "outgoing_path" : exe_obj.outgoing_path,
    "quarantine_path" : "{0}/{1}".format(exe_obj.quarantine_path, get_formatted_time_aet(exe_obj.inputs['process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y%m%d%H%M%S%f",'UTC')),
    "source_name" : exe_obj.inputs['source_app_name'],
    "business_unit_name_code" : exe_obj.inputs['business_unit_name_code'],
    "process_start_time_stamp" : get_formatted_time_aet(exe_obj.inputs['process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y-%m-%d %H:%M:%S.%f",'UTC'),  # convert from UTC to AET
    "work_database" : exe_obj.inputs['work_db_prefix'] + exe_obj.inputs['business_unit_name_code'].lower() + '_work',
    "main_database" : exe_obj.inputs['business_unit_name_code'].lower() + '_brz_' + exe_obj.inputs['source_app_name'].lower(),
    "secret_scope_name" : "intelliversesecrets",
    "encryption_key_name" : "adb-encryption-key"
  }
  return tpl_params

def check_quaran(quaran_dir):
  '''Function reports if any records/files were quarantined'''
  quaran_comment = ''
  in_quarantine = False
  # check if bad_records file path exist in ADLS storage
  try:
    dbutils.fs.ls(quaran_dir + '/bad_records/')
    quaran_comment = quaran_comment + 'Bad records quarantined at: \n' + quaran_dir + '/bad_records/\n'
    in_quarantine = True
  except:
    pass

  # check if bad_files file path exist in ADLS storage
  try:
    dbutils.fs.ls(quaran_dir + '/bad_files/')
    quaran_comment = quaran_comment + 'Bad files quarantined at: \n' + quaran_dir + '/bad_files/'
    in_quarantine = True
  except:
    pass
  
  return (in_quarantine, quaran_comment)

def get_brz_table_metrics_by_file(business_unit_name_code, src, obj, process_start_time_stamp):
  df = sql("""SELECT Source_File_Name, COUNT(*) as Record_Count FROM (SELECT * FROM {0}_brz_{1}.{2} WHERE Year_Month='{3}' AND Process_Start_TimeStamp = '{4}') GROUP BY Source_File_Name""".format(business_unit_name_code,src,obj,datetime.strptime(process_start_time_stamp, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y%m"),process_start_time_stamp))
  return {res['Source_File_Name']: res['Record_Count'] for res in map(lambda row: row.asDict(), df.collect())}

def get_pii_columns(column_tuples_list):
  '''This function returns a tuple containing a list of detministic columns, a list of non-deterministic columns and the total count of PII columns. The input is a list of (column_name, encryption_type) tuples.'''
  det_cols = []
  ndet_cols = []
  for column_name, encryption_type in column_tuples_list:
    if encryption_type == 'DET':
      det_cols.append(column_name)
    elif encryption_type == 'NDET':
      ndet_cols.append(column_name)
  return det_cols,ndet_cols,len(det_cols)+len(ndet_cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run bronze load
# MAGIC * Create instance of Layer Process class
# MAGIC * Initialise file paths and template params
# MAGIC * Render bronze Jinja2 templates and execute SQL queries
# MAGIC * Run PII and Data Quality checks

# COMMAND ----------

try:
  with logging_start("WipToBronze",  dbutils.widgets.get('pipeline_run_id')) as log:
    mandatory = ['pipeline_run_id', 'source_app_name',  'object_name', 'process_start_time_stamp', 'json_schema','business_unit_name_code']
    optional = ['adls_prefix', 'enable_debug_in_databricks', 'work_db_prefix', 'main_db_prefix', 'storage_account']

    # get inputs from ADF, make python dictionary of schema information and validate schema
    lyr_inputs = LayerInputs(mandatory,optional)
    lyr_inputs.validate_schema_dict([(c['ColumnName'], c['DataType']) for c in lyr_inputs.schema_dict['SourceColumns']])
  
    # configure adbfss access to the ADLS storage account for the business unit
    configure_adls_access(lyr_inputs.inputs['business_unit_name_code'])
    
    # start bronze layer execution - kicks off all initial tasks
    brz_exe = TemplateExecutorBronzeSilver('uplift-wip','uplift-bronze','uplift-quarantine', 'brz', lyr_inputs)

    # create python dictionary with additional parameters for template rendering
    tpl_params = get_tpl_params(brz_exe)
    
    with log.operation("Ingest data", business_unit = brz_exe.inputs['business_unit_name_code'], source_app_name=brz_exe.inputs['source_app_name'], object_name=brz_exe.inputs['object_name']) as op_process:
      # start execution of bronze queries
      brz_exe.execute_queries(template=brz_load_start(), template_params=tpl_params, query_delim=';;', log=op_process)
      
      det_cols, ndet_cols, total_pii_cols = get_pii_columns([(c['ColumnName'], c['EncryptionType']) for c in brz_exe.schema_dict['SourceColumns']])
      op_process.info('Encryption information', extra={'total_encrypted_columns':total_pii_cols, 'deterministic_columns':det_cols, 'non-deterministic_columns':ndet_cols})
        
        # Disabled temporarily, to be revisited while handling FPE
        #encrypt_columns(brz_exe.schema_dict['SourceColumns'], brz_exe.inputs['source_app_name'], brz_exe.inputs['object_name'], encryptionTime_int, encryption16BytesString_b, encryptionKey_b, fpeKey, fpeTweak)
      
      dq = DqCheck(brz_exe.inputs['source_app_name'], brz_exe.inputs['object_name'], brz_exe.inputs['pipeline_run_id'], 'brz',brz_exe.inputs['business_unit_name_code'])
      with op_process.operation("Data Quality Check") as op:
        # run data quality checks and end notebook execution if checks failed
        dq.run_dq_check(op)
        op.info("DQ status", extra={"status": dq.dq_status})
        if dq.dq_status == 'fail':
          op.warning('DQ check failed', extra={"dq_message": dq.dq_message})
          raise Exception('Notebook execution stopped due to DQ check: ' + dq.dq_message + '\n' + dq.dq_data_doc_url)

      # complete execution of bronze queries
      brz_exe.execute_queries(template=brz_load_end(), template_params=tpl_params, query_delim=';;', log=op_process)
      
      # log table metrics
      table_metrics = get_table_metrics(tpl_params['outgoing_path'])
      table_metrics['rows_loaded_by_source_file'] = get_brz_table_metrics_by_file(tpl_params['business_unit_name_code'], tpl_params['source_name'], brz_exe.schema_dict['File']['ObjectName'], tpl_params['process_start_time_stamp'])
      op_process.info("Ingestion Metrics", extra=table_metrics)

    # check if any records/files were quarantined
    in_quarantine, quaran_comment = check_quaran(tpl_params['quarantine_path'])
    if in_quarantine:
      log.warning('Some records or files were quarantined', extra = {'quarantine_message':quaran_comment})
    else:
      log.info('No records nor files were quarantined')
  
    # drop worker tables if debug mode is not enabled
    drop_worker_tables(tpl_params['work_database'], tpl_params['source_name'], brz_exe.schema_dict['File']['ObjectName'], brz_exe.inputs['enable_debug_in_databricks'], 'brz', log)

except BaseException as e:
  raise Exception('Notebook execution failed') from e

# COMMAND ----------

# push data_doc_url to ADF
dbutils.notebook.exit(dq.dq_data_doc_url)
