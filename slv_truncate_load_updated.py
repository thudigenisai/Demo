# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Truncate Load Jinja2 template

# COMMAND ----------

def slv_truncate():
  return r"""
-- *----------------------------------------------*
-- STEP 4.1: Truncate Silver Table
-- *----------------------------------------------*
TRUNCATE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;

-- *----------------------------------------------*
-- STEP 4.2: Identify staging records to insert and silver records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;
CREATE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load
	AS
	SELECT 
		stg.`Pk_Hash` 
		,stg.`Row_Hash` 
		,stg.`Effective_Dttm` 
		,stg.`Expiry_Dttm` 
		,stg.`Source_File_Name` 
		,stg.`Source_App_Name` 
		,stg.`Record_Type`
		,stg.`Record_Insert_Dttm` 
		,stg.`Record_Update_Dttm` 
		,stg.`Process_Instance_Id` 
		,stg.`Update_Process_Instance_Id` 
		,stg.`Is_Current` 
		{%- for col in schema_dict['SourceColumns'] -%}
		{##}
		{%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
			{%- if  col['DataType'] == 'TIMESTAMP' -%}
		,stg.`{{col['ColumnName']}}` 
		,stg.`{{col['ColumnName']}}_Aet`
			{%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
		,stg.`{{col['ColumnName']}}` 
		,stg.`{{col['ColumnName']}}_Cpy`
			{%- else -%}
		,stg.`{{col['ColumnName']}}`
			{%- endif -%}  
			{%- endif -%}
		{%- endfor -%}
		{##}
		,stg.`Year_Month`
	FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg;;

ANALYZE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load COMPUTE STATISTICS;;
"""
