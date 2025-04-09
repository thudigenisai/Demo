# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_to_silver_wrapper (Unity Catalog Compatible)

# COMMAND ----------

# MAGIC %run ../utilities/data_quality_validator
# MAGIC %run ../utilities/SecureHash
# MAGIC %run dp-databricks-logging-py/Logging
# MAGIC %run ../patterns/slv_generic_start
# MAGIC %run ../patterns/slv_generic_end
# MAGIC %run ../patterns/slv_generic_brz_load
# MAGIC %run ../patterns/slv_delta_load
# MAGIC %run ../patterns/slv_full_load
# MAGIC %run ../patterns/slv_insert_load
# MAGIC %run ../patterns/slv_truncate_load
# MAGIC %run ../patterns/slv_iud_load
# MAGIC %run ../lib/layer_inputs
# MAGIC %run ../lib/template_executor
# MAGIC %run ../lib/adls_config_executor
# MAGIC %run ../lib/get_table_metrics
# MAGIC %run ../lib/drop_worker_tables
# MAGIC %run ../lib/time

# COMMAND ----------

from datetime import datetime
from pytz import timezone
import json
from jinja2 import Template
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType,StructType
import sys
from delta.tables import *

# COMMAND ----------

def get_brz_max_year_month(catalog_name, business_unit_name_code, source_name, object_name):
  result = sql(f"SHOW PARTITIONS {catalog_name}.{business_unit_name_code}_brz_{source_name}.{object_name}").agg({"Year_Month": "max"}).collect()
  return result[0][0] if result else None

def find_col(source_cols, attribute):
  for col in source_cols:
    if col[attribute]:
      return (True, col['ColumnName'])
  return (False, None)

def get_hash_cols(exe_obj, key_columns_exist):
  hash_cols = {}
  for col in exe_obj.schema_dict['SourceColumns']:
    if key_columns_exist and col['IsKeyColumn']:
      hash_cols[col['ColumnName']] = get_col_tag(col)
    elif not(key_columns_exist) and col['IsBusinessColumn'] and not(col['Ignore']) and not(col['IsKeyColumn']):
      hash_cols[col['ColumnName']] = get_col_tag(col)
  return hash_cols

def get_col_tag(col):
  if col['IsAttributePII'] and col['EncryptionType'] == 'NDET':
    return {'encryption_type': 'NDET', 'data_type': col['DataType']}
  elif col['IsAttributePII'] and col['EncryptionType'] == 'DET':
    return {'encryption_type': 'DET', 'data_type': col['DataType']}
  else:
    return None

def get_tpl_params(exe_obj):
  hist_stitch = find_col(exe_obj.schema_dict['SourceColumns'], 'HistoryStitchColumn')
  hist_stitch_sort = find_col(exe_obj.schema_dict['SourceColumns'], 'HistoryStitchSortKey')
  IUD = find_col(exe_obj.schema_dict['SourceColumns'], 'IsIUDColumn')
  key_columns_exist = find_col(exe_obj.schema_dict['SourceColumns'], 'IsKeyColumn')[0]

  if exe_obj.inputs['load_type'] in ('D','F','IUD'):
    assert key_columns_exist, 'At least one primary key column must exist for Delta, Full & IUD loads'

  return {
    "catalog_name": exe_obj.inputs['catalog_name'],
    "incoming_path": exe_obj.incoming_path,
    "outgoing_path": exe_obj.outgoing_path,
    "sourceName": exe_obj.inputs['source_app_name'],
    "pipelineRunID": exe_obj.inputs['pipeline_run_id'],
    "business_unit_name_code": exe_obj.inputs['business_unit_name_code'],
    "Process_Start_TimeStamp": get_formatted_time_aet(exe_obj.inputs['process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y-%m-%d %H:%M:%S.%f",'UTC'),
    "Prev_Process_Start_TimeStamp": get_formatted_time_aet(exe_obj.inputs['prev_process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y-%m-%d %H:%M:%S.%f",'UTC'),
    "Prev_Process_Start_YearMonth": get_formatted_time_aet(exe_obj.inputs['prev_process_start_time_stamp'],"%Y%m%d%H%M%S%f","%Y%m",'UTC'),
    "HistStitchOn": hist_stitch[0],
    "HistStitchCol": hist_stitch[1],
    "HistStitchSortOn": hist_stitch_sort[0],
    "HistStitchSortCol": hist_stitch_sort[1],
    "IUDOn": IUD[0],
    "IUDCol": IUD[1],
    "load_type": exe_obj.inputs['load_type'],
    "pk_hash_cols": get_hash_cols(exe_obj, key_columns_exist=True),
    "row_hash_cols": get_hash_cols(exe_obj, key_columns_exist=False),
    "work_database": exe_obj.inputs['work_db_prefix'] + exe_obj.inputs['business_unit_name_code'].lower() + '_work',
    "main_database": exe_obj.inputs['business_unit_name_code'].lower() + '_slv_' + exe_obj.inputs['source_app_name'].lower(),
    "secret_scope_name": "acumen-key-vault-scope",
    "encryption_key_name": "adb-encryption-key-v2"
  }

def get_filename_timestamps(exe_obj, tpl_params):
  return sql(f"""
    SELECT DISTINCT Source_Timestamp 
    FROM {tpl_params['catalog_name']}.{exe_obj.inputs['business_unit_name_code']}_brz_{exe_obj.inputs['source_app_name']}.{exe_obj.schema_dict['File']['ObjectName']}
    WHERE Year_Month >= "{tpl_params['Prev_Process_Start_YearMonth']}" AND Process_Start_TimeStamp > "{tpl_params['Prev_Process_Start_TimeStamp']}"
    ORDER BY Source_Timestamp
  """).rdd.map(lambda x: x[0]).collect()

def get_rcount(exe_obj, tpl_params):
  return sql(f"""
    SELECT COUNT(*) FROM {tpl_params['catalog_name']}.{exe_obj.inputs['business_unit_name_code']}_brz_{exe_obj.inputs['source_app_name']}.{exe_obj.schema_dict['File']['ObjectName']}
    WHERE Year_Month >= "{tpl_params['Prev_Process_Start_YearMonth']}" AND Process_Start_TimeStamp > "{tpl_params['Prev_Process_Start_TimeStamp']}" AND {tpl_params['IUDCol']} LIKE "R%"
  """).first()[0]

def get_load_template(load_type):
  if load_type == "D": return slv_delta()
  elif load_type == "I": return slv_insert()
  elif load_type == "IO": return slv_insert_overwrite()
  elif load_type == "T": return slv_truncate()
  elif load_type == "F": return slv_full()
  elif load_type == "IUD": return slv_iud()
  elif load_type == "SPFL": raise Exception("Call Stored Procedure load notebook")
  else: raise Exception(f"Invalid load type: {load_type}")

def run_silver(exe_obj, tpl_params, dq, op_process):
  if exe_obj.inputs['load_type'] == 'T':
    tpl_params['brz_max_year_month'] = get_brz_max_year_month(
      tpl_params['catalog_name'],
      exe_obj.inputs['business_unit_name_code'],
      exe_obj.inputs['source_app_name'],
      exe_obj.schema_dict['File']['ObjectName']
    )

  exe_obj.execute_queries(template=slv_generic_start(), template_params=tpl_params, query_delim=';;', log=op_process)
  exe_obj.execute_queries(template=get_load_template(exe_obj.inputs['load_type']), template_params=tpl_params, query_delim=';;', log=op_process)

  if exe_obj.inputs['load_type'] == 'IUD' and tpl_params['R_Count'] > 0:
    op_process.info("Ingestion Metrics", extra=get_table_metrics(tpl_params['outgoing_path']))
    exe_obj.execute_queries(template=slv_full(), template_params=tpl_params, query_delim=';;', log=op_process)

  exe_obj.execute_queries(template=slv_generic_end(), template_params=tpl_params, query_delim=';;', log=op_process)
  op_process.info("Ingestion Metrics", extra=get_table_metrics(tpl_params['outgoing_path']))

# COMMAND ----------

try:
  with logging_start("BronzeToSilver", dbutils.widgets.get('pipeline_run_id')) as log:
    
    mandatory = ['pipeline_run_id', 'source_app_name', 'object_name', 'process_start_time_stamp', 'json_schema', 'prev_process_start_time_stamp', 'load_type', 'business_unit_name_code', 'catalog_name']
    optional = ['adls_prefix', 'enable_debug_in_databricks', 'work_db_prefix', 'main_db_prefix', 'storage_account']

    lyr_inputs = LayerInputs(mandatory, optional)
    lyr_inputs.validate_schema_dict([(c['ColumnName'], c['DataType']) for c in lyr_inputs.schema_dict['SourceColumns']])
    configure_adls_access(lyr_inputs.inputs['business_unit_name_code'])

    slv_exe = TemplateExecutorBronzeSilver('bronze','silver','quarantine','slv', lyr_inputs)
    tpl_params = get_tpl_params(slv_exe)

    with log.operation("Ingest data", business_unit=slv_exe.inputs['business_unit_name_code'], source_app_name=slv_exe.inputs['source_app_name'], object_name=slv_exe.inputs['object_name'], catalog_name=slv_exe.inputs['catalog_name']) as op_process:
      dq = DqCheck(slv_exe.inputs['source_app_name'], slv_exe.inputs['object_name'], slv_exe.inputs['pipeline_run_id'], 'slv', slv_exe.inputs['business_unit_name_code'])

      op_process.info("Load Type", extra={'load_type': slv_exe.inputs['load_type']})

      if slv_exe.inputs['load_type'] == 'F':
        filename_timestamps = get_filename_timestamps(slv_exe, tpl_params)
        op_process.info("Full load files loaded separately", extra={'file_count': len(filename_timestamps), 'filename_timestamps': filename_timestamps})
        for timestamp in filename_timestamps:
          tpl_params['full_load_source_timestamp'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
          run_silver(slv_exe, tpl_params, dq, op_process)
      elif slv_exe.inputs['load_type'] == 'IUD':
        tpl_params['R_Count'] = int(get_rcount(slv_exe, tpl_params))
        op_process.info("IUD refresh record count", extra={'refresh_record_count': tpl_params['R_Count']})
        run_silver(slv_exe, tpl_params, dq, op_process)
      else:
        run_silver(slv_exe, tpl_params, dq, op_process)

    drop_worker_tables(tpl_params['work_database'], tpl_params['sourceName'], slv_exe.schema_dict['File']['ObjectName'], slv_exe.inputs['enable_debug_in_databricks'], 'slv', log)

except BaseException as e:
  raise Exception('Notebook execution failed') from e

# COMMAND ----------

dbutils.notebook.exit(dq.dq_data_doc_url)
