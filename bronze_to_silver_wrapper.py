# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_to_silver_wrapper
# MAGIC ### This notebook moves data from the bronze container to the silver container.
# MAGIC #### November 2021
# MAGIC Key points:
# MAGIC * This notebook is called by ADF and ADF supplies the input parameters, including the schema information in JSON format.
# MAGIC * General and load specific Jinja2 templates are rendered using the schema and input parameters. This produces a set of SQL queries.
# MAGIC * SQL queries are executed to process the data from bronze to silver.
# MAGIC * Silver table includes primary key columns, business columns (where ignore=false) and audit columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import data quality (GE) functionality

# COMMAND ----------

# MAGIC %run ../utilities/data_quality_validator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Secure Hash functionality

# COMMAND ----------

# MAGIC %run ../utilities/SecureHash

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Logging functionality with Application Insights

# COMMAND ----------

# MAGIC %run dp-databricks-logging-py/Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import silver Jinja2 templates

# COMMAND ----------

# MAGIC %run ../patterns/slv_generic_start

# COMMAND ----------

# MAGIC %run ../patterns/slv_generic_end

# COMMAND ----------

# MAGIC %run ../patterns/slv_generic_brz_load

# COMMAND ----------

# MAGIC %run ../patterns/slv_delta_load

# COMMAND ----------

# MAGIC %run ../patterns/slv_full_load

# COMMAND ----------

# MAGIC %run ../patterns/slv_insert_load

# COMMAND ----------

##%run ../patterns/slv_insert_overwrite_load

# COMMAND ----------

# MAGIC %run ../patterns/slv_truncate_load

# COMMAND ----------

# MAGIC %run ../patterns/slv_iud_load

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import classes

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
from pyspark.sql.types import StringType,StructType
import sys
from delta.tables import *

# COMMAND ----------

def get_brz_max_year_month(business_unit_name_code, source_name, object_name):
  result = sql("SHOW PARTITIONS {0}_brz_{1}.{2}".format(business_unit_name_code, source_name, object_name)).agg({"Year_Month": "max"}).collect()
  if len(result) == 0:
    return None
  else:
    return result[0][0]

def find_col(source_cols, attribute):
  '''Identify & report a source column which has a specified attribute enabled'''
  for col in source_cols:
    if col[attribute]:
      return (True, col['ColumnName'])
  return (False, None)

def get_hash_cols(exe_obj, key_columns_exist):
  '''Return a dictionary of columns to be included in the concatenation for Pk_Hash or Row_Hash'''
  hash_cols = {}
  for col in exe_obj.schema_dict['SourceColumns']:
    if key_columns_exist and col['IsKeyColumn']:
      hash_cols[col['ColumnName']] = get_col_tag(col)   # Pk_Hash columns
    elif not(key_columns_exist) and col['IsBusinessColumn'] and not(col['Ignore']) and not(col['IsKeyColumn']):
      hash_cols[col['ColumnName']] = get_col_tag(col)   # Row_Hash columns
  return hash_cols

def get_col_tag(col):
  '''Return a tag indicating if decryption is required for the specified column'''
  if col['IsAttributePII'] and col['EncryptionType']=='NDET':
    return {'encryption_type': 'NDET', 'data_type': col['DataType']}
  elif col['IsAttributePII'] and col['EncryptionType']=='DET':
    return {'encryption_type': 'DET', 'data_type': col['DataType']}
  else:
    return None

def get_tpl_params(exe_obj):
  '''Create a python dictionary that will be used in addition to the schema dictionary to render Jinja2 templates'''
  hist_stitch = find_col(exe_obj.schema_dict['SourceColumns'],'HistoryStitchColumn')
  hist_stitch_sort = find_col(exe_obj.schema_dict['SourceColumns'],'HistoryStitchSortKey')
  IUD = find_col(exe_obj.schema_dict['SourceColumns'],'IsIUDColumn')
  key_columns_exist = find_col(exe_obj.schema_dict['SourceColumns'],'IsKeyColumn')[0]
  
  if exe_obj.inputs['load_type'] in ('D','F','IUD'):
    assert key_columns_exist, 'At least one primary key column must exist for Delta, Full & IUD loads'
  
  tpl_params = {
    "incoming_path" : exe_obj.incoming_path,
    "outgoing_path" : exe_obj.outgoing_path,
    "sourceName" : exe_obj.inputs['source_app_name'],
    "pipelineRunID" : exe_obj.inputs['pipeline_run_id'],
    "business_unit_name_code" : exe_obj.inputs['business_unit_name_code'],
    "Process_Start_TimeStamp" : get_formatted_time_aet(exe_obj.inputs['process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y-%m-%d %H:%M:%S.%f",'UTC'),  # convert from UTC to AET
    "Prev_Process_Start_TimeStamp" : get_formatted_time_aet(exe_obj.inputs['prev_process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y-%m-%d %H:%M:%S.%f",'UTC'),  # convert from UTC to AET
    "Prev_Process_Start_YearMonth" : get_formatted_time_aet(exe_obj.inputs['prev_process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y%m",'UTC'),  # convert from UTC to AET
    "HistStitchOn" : hist_stitch[0],
    "HistStitchCol" : hist_stitch[1],
    "HistStitchSortOn" : hist_stitch_sort[0],
    "HistStitchSortCol" : hist_stitch_sort[1],
    "IUDOn" : IUD[0],
    "IUDCol" : IUD[1],
    "load_type" : exe_obj.inputs['load_type'],
    "pk_hash_cols" : get_hash_cols(exe_obj, key_columns_exist=key_columns_exist),
    "row_hash_cols" : get_hash_cols(exe_obj, key_columns_exist=False),
    "work_database" : exe_obj.inputs['work_db_prefix'] + exe_obj.inputs['business_unit_name_code'].lower() + '_work',
    "main_database" : exe_obj.inputs['business_unit_name_code'].lower() + '_slv_' + exe_obj.inputs['source_app_name'].lower(),
    "secret_scope_name" : "acumen-key-vault-scope",
    "encryption_key_name" : "adb-encryption-key-v2"
  }
  return tpl_params

def get_filename_timestamps(exe_obj, tpl_params):
  '''Get an ordered list of filename timestamps for full load only'''
  return sql('SELECT DISTINCT Source_Timestamp FROM {0}_brz_{1}.{2}'.format(exe_obj.inputs['business_unit_name_code'], exe_obj.inputs['source_app_name'], exe_obj.schema_dict['File']['ObjectName']) + ' WHERE Year_Month >="'+tpl_params['Prev_Process_Start_YearMonth']+'" AND Process_Start_TimeStamp>"'+tpl_params['Prev_Process_Start_TimeStamp']+'" ORDER BY Source_Timestamp').rdd.map(lambda x: x[0]).collect()

def get_rcount(exe_obj, tpl_params):
  '''Get a count of how many records to process are marked as 'Refresh' for IUD load only'''
  assert len(tpl_params['IUDCol'])>0, 'IUD tag column is not nominated'
  return sql('SELECT Count(*) FROM {0}_brz_{1}.{2}'.format(exe_obj.inputs['business_unit_name_code'], exe_obj.inputs['source_app_name'], exe_obj.schema_dict['File']['ObjectName']) + ' WHERE Year_Month >="'+tpl_params['Prev_Process_Start_YearMonth']+'" AND Process_Start_TimeStamp>"'+tpl_params['Prev_Process_Start_TimeStamp']+'" AND '+tpl_params['IUDCol']+' like "R%"').first()[0]

def get_load_template(load_type):
  '''Returns the Jinja2 template for the specified load type'''
  if load_type=="D":
    template = slv_delta()
  elif load_type=="I":
    template = slv_insert()
  elif load_type=="IO":
    template = slv_insert_overwrite()
  elif load_type=="T":
    template = slv_truncate()
  elif load_type=="F":
    template = slv_full()
  elif load_type=="IUD":
    template = slv_iud()
  elif load_type=="SPFL":
    raise Exception("Call Stored Procedure load notebook")
  else:
    raise Exception('Invalid load type: ' + load_type)
  return template

def run_silver(exe_obj, tpl_params, dq, op_process):
  '''Runs silver patterns'''
  # load bronze table
  ##exe_obj.execute_queries(template=slv_generic_brz_load(), template_params=tpl_params, query_delim=';;', log=op_process)
  
  # get maximum Year_Month of bronze table for truncate load only
  if exe_obj.inputs['load_type'] == 'T':
    tpl_params['brz_max_year_month'] = get_brz_max_year_month(exe_obj.inputs['business_unit_name_code'], exe_obj.inputs['source_app_name'], exe_obj.schema_dict['File']['ObjectName'])
  
  # start execution of generic silver queries
  exe_obj.execute_queries(template=slv_generic_start(), template_params=tpl_params, query_delim=';;', log=op_process)
  
#   with op_process.operation("Data Quality Check") as op:
#     # run data quality checks and end notebook execution if checks failed
#     dq.run_dq_check(op)
#     op.info("DQ status", extra={"status": dq.dq_status})
#     if dq.dq_status == 'fail':
#       op.warning('DQ check failed', extra={"dq_message": dq.dq_message})
#       raise Exception('Notebook execution stopped due to DQ check: ' + dq.dq_message + dq.dq_data_doc_url)
  
  # execute load specific queries
  exe_obj.execute_queries(template=get_load_template(exe_obj.inputs['load_type']), template_params=tpl_params, query_delim=';;', log=op_process)
  
  # for IUD load, run full load queries after IUD quereis if there are refresh records
  if exe_obj.inputs['load_type'] == 'IUD' and tpl_params['R_Count'] > 0:
    op_process.info("Ingestion Metrics", extra=get_table_metrics(tpl_params['outgoing_path']))
    exe_obj.execute_queries(template=slv_full(), template_params=tpl_params, query_delim=';;', log=op_process)
  
  # complete execution of generic silver queries
  exe_obj.execute_queries(template=slv_generic_end(), template_params=tpl_params, query_delim=';;', log=op_process)
  op_process.info("Ingestion Metrics", extra=get_table_metrics(tpl_params['outgoing_path']))

# COMMAND ----------

try:
  with logging_start("BronzeToSilver",  dbutils.widgets.get('pipeline_run_id')) as log:
    
    mandatory = ['pipeline_run_id', 'source_app_name',  'object_name', 'process_start_time_stamp', 'json_schema', 'prev_process_start_time_stamp', 'load_type', 'business_unit_name_code']
    optional = ['adls_prefix','enable_debug_in_databricks', 'work_db_prefix', 'main_db_prefix', 'storage_account']
    
    # get inputs from ADF, make python dictionary of schema information and validate schema
    lyr_inputs = LayerInputs(mandatory,optional)
    lyr_inputs.validate_schema_dict([(c['ColumnName'], c['DataType']) for c in lyr_inputs.schema_dict['SourceColumns']])
    
    # configure adbfss access to the ADLS storage account for the business unit
    configure_adls_access(lyr_inputs.inputs['business_unit_name_code'])
    
    # create a layer process - kicks off all initial tasks
    slv_exe = TemplateExecutorBronzeSilver('bronze','silver','quarantine', 'slv', lyr_inputs)

    # create python dictionary with additional parameters for template rendering
    tpl_params = get_tpl_params(slv_exe)

    with log.operation("Ingest data", business_unit = slv_exe.inputs['business_unit_name_code'], source_app_name=slv_exe.inputs['source_app_name'], object_name=slv_exe.inputs['object_name']) as op_process:
      # initiate dq class for data quality checks
      dq = DqCheck(slv_exe.inputs['source_app_name'], slv_exe.inputs['object_name'], slv_exe.inputs['pipeline_run_id'], 'slv',slv_exe.inputs['business_unit_name_code'])

      # Execute silver patterns
      op_process.info("Load Type", extra={'load_type':slv_exe.inputs['load_type']})
      
      if slv_exe.inputs['load_type'] == 'F':
        # For full load, get list of filename timestamps and load each source file separately from bronze, in order of filename timestamp
        filename_timestamps = get_filename_timestamps(slv_exe, tpl_params)
        op_process.info("Full load files loaded separately", extra={'file_count': len(filename_timestamps), 'filename_timestamps':filename_timestamps})
        for timestamp in filename_timestamps:
          tpl_params['full_load_source_timestamp'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
          run_silver(slv_exe, tpl_params, dq, op_process)
      elif slv_exe.inputs['load_type']=='IUD':
        # For IUD load, get a count of refresh records
        tpl_params['R_Count'] = int(get_rcount(slv_exe, tpl_params))
        op_process.info("IUD refresh record count", extra={'refresh_record_count':tpl_params['R_Count']})
        run_silver(slv_exe, tpl_params, dq, op_process)
      else:
        # No special handling required for other load types
        run_silver(slv_exe, tpl_params, dq, op_process)

    # drop worker tables if debug mode is not enabled
    drop_worker_tables(tpl_params['work_database'], tpl_params['sourceName'], slv_exe.schema_dict['File']['ObjectName'], slv_exe.inputs['enable_debug_in_databricks'], 'slv', log)

except BaseException as e:
  raise Exception('Notebook execution failed') from e

# COMMAND ----------

# push data_doc_url to ADF
dbutils.notebook.exit(dq.dq_data_doc_url)
