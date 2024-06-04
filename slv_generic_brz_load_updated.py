# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Generic Load Jinja2 template

# COMMAND ----------

def slv_generic_brz_load():
  return r"""
-- *----------------------------------------------*
-- STEP 0: Load bronze table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['business_unit_name_code']}}_brz_{{template_params['sourceName']|lower}}.{{schema_dict['File']['ObjectName']}};;
CREATE TABLE {{template_params['business_unit_name_code']}}_brz_{{template_params['sourceName']|lower}}.{{schema_dict['File']['ObjectName']}}
	({% for col in schema_dict['SourceColumns'] %}
      {% if loop.index > 1 %},{%- endif -%}
      {%- if col['IsAttributePII'] == True -%}
      `{{col['ColumnName']}}` BINARY
      {%- elif col['DataType'] == 'TIMESTAMP' -%}
      `{{col['ColumnName']}}` STRING
      {%- else -%}
      `{{col['ColumnName']}}` {{col['DataType']}}
      {%- endif -%}
	 {% endfor %}
      ,`Source_Date` DATE
      ,`Source_Timestamp` TIMESTAMP
      ,`Process_Start_TimeStamp` TIMESTAMP
      ,`Source_File_Name` STRING
      ,`Year_Month` INT
	)
USING DELTA
PARTITIONED BY (Year_Month)
LOCATION '{{template_params['incoming_path']}}';;

ANALYZE TABLE {{template_params['business_unit_name_code']}}_brz_{{template_params['sourceName']|lower}}.{{schema_dict['File']['ObjectName']}} COMPUTE STATISTICS;;
"""
