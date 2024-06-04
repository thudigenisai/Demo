# Databricks notebook source
# MAGIC %md
# MAGIC ##### Bronze Load Jinja2 template

# COMMAND ----------

def brz_load_end():
    return r"""
-- *----------------------------------------------*
-- STEP 3: Create/Load existing bronze table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;
CREATE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}}
	(
	{% for col in schema_dict['SourceColumns'] %}
		{% if loop.index > 1 %},{%- endif -%}
        {%- if col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
      `{{col['ColumnName']}}` STRING
      ,`{{col['ColumnName']}}_Cpy` STRING
        {%- elif col['IsAttributePII'] == True -%}
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
LOCATION '{{template_params['outgoing_path']}}';;

-- *----------------------------------------------*
-- STEP 4: Delete any rows in bronze table that have the same Source_Filename as any row in staging table
-- *----------------------------------------------*
DELETE FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as brz
WHERE EXISTS(SELECT 1 FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE stg.`Source_File_Name`=brz.`Source_File_Name`);;

-- *----------------------------------------------*
-- STEP 5: Merge staging table with bronze table (append all rows)
-- *----------------------------------------------*

INSERT INTO {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}}
PARTITION (Year_Month)
SELECT 
	*
FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg;;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} COMPUTE STATISTICS;;

{#
-- *----------------------------------------------*
-- STEP 6: Create dynamic view for decrypting PII data for priviliged users
-- *----------------------------------------------*
 DROP VIEW IF EXISTS {{template_params['main_database']}}_piiView.{{schema_dict['File']['ObjectName']}};; 
 #}
 
 {# 
 CREATE VIEW {{template_params['main_database']}}_piiView.{{schema_dict['File']['ObjectName']}}
 as SELECT 
       {% for col in schema_dict['SourceColumns'] %}
 		{% if loop.index > 1 %},{%- endif -%}
         {%- if col['IsAttributePII'] == True -%}
         
           {%- if col['EncryptionType'] == 'FPE' -%}
 	case when is_member("test_dynamic_view") then udfDecryptAesStatic(`{{col['ColumnName']}}_Cpy`,"${spark.sql.fernet}") else `{{col['ColumnName']}}` end as `{{col['ColumnName']}}`
          {%- elif col['EncryptionType'] == 'NDET'  -%}
              `{{col['ColumnName']}}`,
               case {% for rbac_group in col['RBAC_Code'].split(',') %}
                   when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_ndet(`{{col['ColumnName']}}`) as {{col['DataType']}} )
               {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
               
         {%- elif col['EncryptionType'] == 'DET' -%}
             `{{col['ColumnName']}}`,
               case {% for rbac_group in col['RBAC_Code'].split(',') %}
               when is_member('{{rbac_group}}') then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_det(`{{col['ColumnName']}}`) as {{col['DataType']}} )
                {% endfor %} else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_pii`
               
          {%-endif-%}
         {%- else -%}
 	`{{col['ColumnName']}}`
         {%- endif -%}
 	{% endfor %}
 	 ,`Source_Date`
 	 ,`Source_Timestamp`
 	 ,`Process_Start_TimeStamp`
 	 ,`Source_File_Name`
 	 ,`Year_Month`
 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};; 
 #}
"""
