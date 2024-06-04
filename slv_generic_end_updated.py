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
		{%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
	,`{{col['ColumnName']}}` 
	,`{{col['ColumnName']}}_Cpy`
		{%- else -%}
	,`{{col['ColumnName']}}`
		{%- endif -%}  
      {%- endif -%}
	{%- endfor -%}
	 {##}
	,`Year_Month`
FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} COMPUTE STATISTICS;;
"""

# COMMAND ----------

# MAGIC %md
# MAGIC -- *----------------------------------------------*
# MAGIC -- STEP 6: Create dynamic view for decrypting PII data for priviliged users
# MAGIC -- *----------------------------------------------*
# MAGIC DROP VIEW IF EXISTS {{template_params['main_database']}}_piiView.{{schema_dict['File']['ObjectName']}};;
# MAGIC
# MAGIC CREATE VIEW {{template_params['main_database']}}_piiView.{{schema_dict['File']['ObjectName']}}
# MAGIC as SELECT `Pk_Hash` 
# MAGIC 	 ,`Row_Hash` 
# MAGIC 	 ,`Effective_Dttm`
# MAGIC 	 ,`Expiry_Dttm` 
# MAGIC 	 ,`Source_File_Name` 
# MAGIC 	 ,`Source_App_Name` 
# MAGIC 	 ,`Record_Type` 
# MAGIC 	 ,`Record_Insert_Dttm` 
# MAGIC 	 ,`Record_Update_Dttm` 
# MAGIC 	 ,`Process_Instance_Id` 
# MAGIC 	 ,`Update_Process_Instance_Id` 
# MAGIC 	 ,`Is_Current` 
# MAGIC 	{% for col in schema_dict['SourceColumns'] %}
# MAGIC 	 {##}
# MAGIC 	  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
# MAGIC 		{%- if  col['DataType'] == 'TIMESTAMP' -%}
# MAGIC           {%- if col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
# MAGIC     ,`{{col['ColumnName']}}`
# MAGIC     ,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC             when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_ndet(`{{col['ColumnName']}}`) as {{col['DataType']}} )
# MAGIC           {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
# MAGIC 	,`{{col['ColumnName']}}_Aet`
# MAGIC     ,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC             when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_ndet(`{{col['ColumnName']}}_Aet`) as {{col['DataType']}} )
# MAGIC             {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_Aet_pii`
# MAGIC           {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
# MAGIC     ,`{{col['ColumnName']}}`
# MAGIC     ,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC             when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_det(`{{col['ColumnName']}}`) as {{col['DataType']}} )
# MAGIC     {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
# MAGIC 	,`{{col['ColumnName']}}_Aet`
# MAGIC     ,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC            when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_det(`{{col['ColumnName']}}_Aet`) as {{col['DataType']}} )
# MAGIC     {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_Aet_pii`
# MAGIC           {%- else -%}
# MAGIC 	,`{{col['ColumnName']}}`
# MAGIC 	,`{{col['ColumnName']}}_Aet`
# MAGIC           {%- endif -%}
# MAGIC 		{%- elif col['IsAttributePII'] == True -%}
# MAGIC 				{%- if col['EncryptionType'] == 'FPE' -%}
# MAGIC 	,case when is_member("test_dynamic_view") then udfDecryptAesStatic(`{{col['ColumnName']}}_Cpy`,"${spark.sql.fernet}") else `{{col['ColumnName']}}` end as `{{col['ColumnName']}}`
# MAGIC           {%- elif col['EncryptionType'] == 'NDET'  -%}
# MAGIC     ,`{{col['ColumnName']}}`
# MAGIC 	,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC         when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_ndet(`{{col['ColumnName']}}`) as {{col['DataType']}} )
# MAGIC     {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
# MAGIC           {%- elif col['EncryptionType'] == 'DET'  -%}
# MAGIC     ,`{{col['ColumnName']}}`
# MAGIC 	,case {% for rbac_group in col['RBAC_Code'].split(',') %}
# MAGIC         when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_det(`{{col['ColumnName']}}`) as {{col['DataType']}} )
# MAGIC     {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
# MAGIC           {%- endif -%}
# MAGIC 		{%- else -%}
# MAGIC 	,`{{col['ColumnName']}}`
# MAGIC 		{%- endif -%}
# MAGIC 	  {%- endif -%}
# MAGIC 	{%- endfor -%}
# MAGIC 	 {##}
# MAGIC 	 ,`Year_Month` 
# MAGIC FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;
