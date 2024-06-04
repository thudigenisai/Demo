# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Generic Load Jinja2 template

# COMMAND ----------

def slv_generic_end():
  return r"""
-- *----------------------------------------------*
-- STEP 5: Insert all records identified in step 4 into the silver table
-- *----------------------------------------------*
INSERT INTO {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}}
PARTITION (`Year_Month`)
SELECT 
	`Pk_Hash` 
	 ,`Row_Hash` 
	 ,`Effective_Dttm` 
	 ,`Expiry_Dttm` 
	 ,`Source_File_Name` 
	 ,`Source_App_Name` 
	 ,`Record_Type`
	 ,`Record_Insert_Dttm` 
	 ,`Record_Update_Dttm` 
	 ,`Process_Instance_Id` 
	 ,`Update_Process_Instance_Id` 
	 ,`Is_Current` 
	{% for col in schema_dict['SourceColumns'] %}
	 {##}
	  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
		{%- if  col['DataType'] == 'TIMESTAMP' -%}
	,`{{col['ColumnName']}}` 
	,`{{col['ColumnName']}}_Aet`
		{%- else -%}
	,`{{col['ColumnName']}}`
		{%- endif -%}  
      {%- endif -%}
	{%- endfor -%}
	 {##}
	,`Year_Month`
FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 6: Create dynamic view for decrypting PII data for priviliged users
-- *----------------------------------------------*
CREATE OR REPLACE VIEW {{template_params['main_database']}}_vw.{{schema_dict['File']['ObjectName']}}
as SELECT * FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;

CREATE OR REPLACE VIEW {{template_params['main_database']}}_vw.{{schema_dict['File']['ObjectName']}}_decrypt
as SELECT `Pk_Hash` 
	,`Row_Hash` 
	,`Effective_Dttm`
	,`Expiry_Dttm` 
	,`Source_File_Name` 
	,`Source_App_Name` 
	,`Record_Type` 
	,`Record_Insert_Dttm` 
	,`Record_Update_Dttm` 
	,`Process_Instance_Id` 
	,`Update_Process_Instance_Id` 
	,`Is_Current` 
	{% for col in schema_dict['SourceColumns'] %}
	{##}
	  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
		{%- if  col['DataType'] == 'TIMESTAMP' -%}
          {%- if col['IsAttributePII'] == True -%}
    ,`{{col['ColumnName']}}`
    ,case when is_member("pii-privileged") then cast(cast(aes_decrypt(`{{col['ColumnName']}}`, secret('{{template_params['secret_scope_name']}}', '{{template_params['encryption_key_name']}}'),{% if col['EncryptionType'] == 'NDET' %}'GCM'{% else %}'ECB'{% endif %}) as STRING) as {{col['DataType']}})
    else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_decrypt`
	,`{{col['ColumnName']}}_Aet`
    ,case when is_member("pii-privileged") then cast(cast(aes_decrypt(`{{col['ColumnName']}}_Aet`, secret('{{template_params['secret_scope_name']}}', '{{template_params['encryption_key_name']}}'),{% if col['EncryptionType'] == 'NDET' %}'GCM'{% else %}'ECB'{% endif %}) as STRING) as {{col['DataType']}})
    else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_Aet_decrypt`
          {%- else -%}
	,`{{col['ColumnName']}}`
	,`{{col['ColumnName']}}_Aet`
          {%- endif -%}
		{%- elif col['IsAttributePII'] == True -%}
    ,`{{col['ColumnName']}}`
    ,case when is_member("pii-privileged") then cast(cast(aes_decrypt(`{{col['ColumnName']}}`, secret('{{template_params['secret_scope_name']}}', '{{template_params['encryption_key_name']}}'),{% if col['EncryptionType'] == 'NDET' %}'GCM'{% else %}'ECB'{% endif %}) as STRING) as {{col['DataType']}})
    else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_decrypt`
		{%- else -%}
	,`{{col['ColumnName']}}`
		{%- endif -%}
	  {%- endif -%}
	{%- endfor -%}
	{##}
	,`Year_Month` 
FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;
"""
