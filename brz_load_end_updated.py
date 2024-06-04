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

-- *----------------------------------------------*
-- STEP 6: Create views (1-> for all users with no decryption. 2-> for pii-privileged users with decryption.)
-- *----------------------------------------------*
CREATE OR REPLACE VIEW {{template_params['main_database']}}_vw.{{schema_dict['File']['ObjectName']}}
as SELECT * FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;

CREATE OR REPLACE VIEW {{template_params['main_database']}}_vw.{{schema_dict['File']['ObjectName']}}_decrypt
as SELECT 
  {% for col in schema_dict['SourceColumns'] %}
		{% if loop.index > 1 %},{%- endif -%}
      {%- if col['IsAttributePII'] == True -%}
  `{{col['ColumnName']}}`,
	case when is_member("pii-privileged") then cast(cast(aes_decrypt(`{{col['ColumnName']}}`, secret('{{template_params['secret_scope_name']}}', '{{template_params['encryption_key_name']}}'),{% if col['EncryptionType'] == 'NDET' %}'GCM'{% else %}'ECB'{% endif %}) as STRING) as {{col['DataType']}})
    else cast(NULL as {{col['DataType']}}) end as `{{col['ColumnName']}}_decrypt`
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
"""
