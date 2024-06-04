# Databricks notebook source
# MAGIC %md
# MAGIC ##### Bronze Load Jinja2 template

# COMMAND ----------

def brz_load_start():
  return r"""
-- *----------------------------------------------*
-- STEP 1: Read in wip file(s) to wip table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_wip;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_wip
{##}{%- if (schema_dict['File']['FileExtension']|lower) in ('csv', 'txt', 'tsv') -%}
    (
	{%- for col in schema_dict['SourceColumns'] -%}{##}
		{% if loop.index > 1 %},{%- endif -%}
		`{{col['ColumnName']}}` STRING
	{%- endfor -%}{##}
	)
USING CSV
OPTIONS (path '{{template_params['incoming_path']}}',sep '{{schema_dict['File']['FieldDelimiter']}}',header '{{schema_dict['File']['HasHeader']}}', multiline{% if schema_dict['File']['ContainsMultilineData'] %} 'true'{% else %} 'false'{% endif %}, ignoreTrailingWhiteSpace 'true', quote '\"',escape{% if schema_dict['File']['EscapeCharacter']=='\\' %} '\{{schema_dict['File']['EscapeCharacter']}}'{% else %} '{{schema_dict['File']['EscapeCharacter']}}'{% endif %},{% if  schema_dict['File']['FileEncoding']!='AUTO' %} encoding '{{schema_dict['File']['FileEncoding']}}',{% endif %} badRecordsPath '{{template_params['quarantine_path']}}');;
{##}{%- else -%}
USING {{schema_dict['File']['FileExtension']}}
OPTIONS (multiline{% if schema_dict['File']['ContainsMultilineData'] %} 'true'{% else %} 'false'{% endif %}, badRecordsPath '{{template_params['quarantine_path']}}')
LOCATION '{{template_params['incoming_path']}}';;
{%- endif -%}
{##}
ANALYZE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_wip COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 2: Create staging table with bronze layer columns
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg
	USING DELTA
	AS 
	SELECT
	{% for col in schema_dict['SourceColumns'] %}
	  {% if loop.index > 1 %},{%- endif -%}
      {%- if col['IsAttributePII'] == True -%}
      aes_encrypt(cast(cast({% if col['DataType'] == 'DATE' %}to_date(`{{col['ColumnName']}}`,'{{col['Format']['InputFormatString']}}'){% else %}`{{col['ColumnName']}}`{% endif %} as STRING) as BINARY), secret('{{template_params['secret_scope_name']}}', '{{template_params['encryption_key_name']}}'),{% if col['EncryptionType'] == 'NDET' %}'GCM'{% else %}'ECB'{% endif %}) as `{{col['ColumnName']}}`
      {%- elif col['DataType'] == 'TIMESTAMP' -%}
      CAST(`{{col['ColumnName']}}` AS STRING) as `{{col['ColumnName']}}`
      {%- elif col['DataType'] == 'DATE' -%}
      to_date(`{{col['ColumnName']}}`,'{{col['Format']['InputFormatString']}}') as `{{col['ColumnName']}}`
      {%- else -%}
      CAST(`{{col['ColumnName']}}` AS {{col['DataType']}}) as `{{col['ColumnName']}}`
      {%- endif -%}
	{% endfor %}
      ,CAST(from_unixtime(to_unix_timestamp(regexp_extract(reverse(split(input_file_name(),'/'))[0],'{{schema_dict['File']['TimeStampFilenameRegexPattern']}}', 1),'{{schema_dict['File']['TimeStampFilenamePattern']}}'),'yyyy-MM-dd HH:mm:ss') AS DATE) AS `Source_Date`
      ,CAST(from_unixtime(to_unix_timestamp(regexp_extract(reverse(split(input_file_name(),'/'))[0],'{{schema_dict['File']['TimeStampFilenameRegexPattern']}}', 1),'{{schema_dict['File']['TimeStampFilenamePattern']}}'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS `Source_Timestamp`
      ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) as `Process_Start_TimeStamp`
      ,reverse(split(input_file_name(),'/'))[0] AS `Source_File_Name`
      ,CAST(date_format(timestamp '{{template_params['process_start_time_stamp']}}','yyyyMM') as INT) as `Year_Month` 
    FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_wip;;

ANALYZE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg COMPUTE STATISTICS;;
"""
