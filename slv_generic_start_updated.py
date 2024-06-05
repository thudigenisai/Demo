# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Generic Load Jinja2 template

# COMMAND ----------

def slv_generic_start():
  return r"""
-- *----------------------------------------------*
-- STEP 1: Create Staging table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg;;
CREATE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg 
	USING DELTA
	AS
    SELECT * 
            ,ROW_NUMBER() OVER(PARTITION BY `Pk_Hash`, `Row_Hash`, (CASE WHEN `Record_Type`='D' THEN `Record_Type` ELSE NULL END) ORDER BY `Effective_Dttm` DESC) AS `Dup_Cnt`  
    FROM
	(SELECT 
 			secure_hash(array(
			{%- for col, tag in template_params['pk_hash_cols'].items() -%}
              {%- if loop.index > 1 -%},{%- endif -%}
              {%- if tag['encryption_type'] == 'NDET' -%}
                COALESCE(decrypt_scala_ndet_binary(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}`),"")
              {%- elif tag['encryption_type'] == 'DET' -%}
                COALESCE(decrypt_scala_det_binary(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}`),"")
              {%- else  -%}
                COALESCE(cast(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}` as STRING),"")
              {%- endif -%}
              {##}
			{% endfor %}),'|')  AS `Pk_Hash`
        ,secure_hash(array(
			{%- for col, tag in template_params['row_hash_cols'].items() -%}
			  {%- if loop.index > 1 -%},{%- endif -%}
              {%- if tag['encryption_type'] == 'NDET' -%}
                COALESCE(decrypt_scala_ndet_binary(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}`),"")
              {%- elif tag['encryption_type'] == 'DET' -%}
                COALESCE(decrypt_scala_det_binary(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}`),"")
              {%- else  -%}
                COALESCE(cast(b.`{{col}}{%- if tag['data_type']=='TIMESTAMP' -%}_Aet{%- endif -%}` as STRING),"")
              {%- endif -%}
              {##}
			{% endfor %}),'|')  AS `Row_Hash`
        {##}{%- if template_params['HistStitchOn'] -%}
		,CAST(b.`{{template_params['HistStitchCol']}}_Aet` as TIMESTAMP)  AS `Effective_Dttm`
		{%- else -%}
		,b.`Effective_Dttm` AS `Effective_Dttm` 
		{%- endif -%}{##}
        ,b.`Expiry_Dttm`
        ,b.`Source_File_Name`
        ,b.`Source_App_Name`
        ,b.`Record_Type`
        ,b.`Record_Insert_Dttm`
        ,b.`Record_Update_Dttm`
        ,b.`Process_Instance_Id`
        ,b.`Update_Process_Instance_Id`
        ,b.`Is_Current`
        {##}{%- for col in schema_dict['SourceColumns'] -%}
		  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%}
            {%- if  col['DataType'] == 'TIMESTAMP' -%}
        ,b.`{{col['ColumnName']}}`
        ,b.`{{col['ColumnName']}}_Aet`{##}
             {%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
        ,b.`{{col['ColumnName']}}`
        ,b.`{{col['ColumnName']}}_Cpy`{##}
             {%- else -%}
        ,b.`{{col['ColumnName']}}`{##}
             {%- endif -%}  
		  {%- endif -%}
		{%- endfor -%}
        {%- if template_params['load_type']=='F' or template_params['load_type']=='IUD' -%}
        ,b.`Source_Timestamp`
        {%- endif -%}
		,b.`Year_Month`
	FROM 
	(
	SELECT
		`Source_Timestamp` AS `Effective_Dttm` 
		,CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS `Expiry_Dttm`
		,Source_File_Name AS `Source_File_Name`
		,'{{template_params['sourceName']}}' AS `Source_App_Name`{##}
		{##}{%- if template_params['load_type']=='IUD' and template_params['IUDOn'] -%}
		, CASE WHEN `{{template_params['IUDCol']}}` == '{{schema_dict['File']['DeleteCDCValue']}}' THEN 'D'
               WHEN `{{template_params['IUDCol']}}` == '{{schema_dict['File']['InsertCDCValue']}}' THEN 'I'
               WHEN `{{template_params['IUDCol']}}` == '{{schema_dict['File']['UpdateCDCValue']}}' THEN 'U'
               ELSE upper(substring(`{{template_params['IUDCol']}}`,0,1))
          END AS `Record_Type`
		 {%- else -%} 
		,'I' AS `Record_Type`
		 {%- endif -%}{##}
		,CAST('{{template_params['Process_Start_TimeStamp']}}' AS TIMESTAMP) AS `Record_Insert_Dttm`
		,CAST(null AS TIMESTAMP) AS `Record_Update_Dttm` 
		,'{{template_params['pipelineRunID']}}' AS `Process_Instance_Id` 
		,CAST(null AS TIMESTAMP) AS `Update_Process_Instance_Id` 
		,CAST(1 AS BOOLEAN) AS `Is_Current`
		{##}{%- for col in schema_dict['SourceColumns'] -%}
		  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
		  	{%- if  col['Nullable'] -%}
              {%- if  col['DataType'] == 'TIMESTAMP' and col['Format']['TimeZone']!=None -%}
		,`{{col['ColumnName']}}`
        {%-if col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
        ,encrypt_scala_det_binary(cast(from_utc_timestamp(to_utc_timestamp(to_timestamp(decrypt_scala_det_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne") as STRING)) AS `{{col['ColumnName']}}_Aet`
        {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
        ,encrypt_scala_ndet_binary(cast(from_utc_timestamp(to_utc_timestamp(to_timestamp(decrypt_scala_ndet_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne")as STRING)) AS `{{col['ColumnName']}}_Aet`
        {%- else -%}
        ,from_utc_timestamp(to_utc_timestamp(to_timestamp(`{{col['ColumnName']}}`, '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne") AS `{{col['ColumnName']}}_Aet`
        {%- endif -%}
        {##}
              {%- elif  col['DataType'] == 'TIMESTAMP' and col['Format']['TimeZone']==None -%}
		,`{{col['ColumnName']}}`
        {%-if col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
        ,encrypt_scala_det_binary(cast(from_utc_timestamp(to_timestamp(decrypt_scala_det_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne")as STRING)) AS `{{col['ColumnName']}}_Aet`
        {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
        ,encrypt_scala_ndet_binary(cast(from_utc_timestamp(to_timestamp(decrypt_scala_ndet_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne")as STRING)) AS `{{col['ColumnName']}}_Aet`
        {%- else -%}
        ,from_utc_timestamp(to_timestamp(`{{col['ColumnName']}}`, '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne") AS `{{col['ColumnName']}}_Aet`
        {%- endif -%}
        {##}
              {%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
		,`{{col['ColumnName']}}`
		,`{{col['ColumnName']}}_Cpy`
		{##}
              {%- else -%}
		,`{{col['ColumnName']}}`
		  {##}
              {%- endif -%}
		  	{%- else -%}
              {%- if  col['DataType'] == 'TIMESTAMP' and col['Format']['TimeZone']!=None  -%}
        {%-if col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_det_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
        ,COALESCE(encrypt_scala_det_binary(cast(from_utc_timestamp(to_utc_timestamp(to_timestamp(decrypt_scala_det_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne") as STRING)), encrypt_scala_det_binary(CAST('{{col['DefaultValueIfNotNull']}}' as STRING))) AS `{{col['ColumnName']}}_Aet`
        {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_ndet_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
        ,COALESCE(encrypt_scala_ndet_binary(cast(from_utc_timestamp(to_utc_timestamp(to_timestamp(decrypt_scala_ndet_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne")as STRING)), encrypt_scala_ndet_binary(CAST('{{col['DefaultValueIfNotNull']}}' as STRING))) AS `{{col['ColumnName']}}_Aet`
        {%- else -%}
		,nvl(`{{col['ColumnName']}}`,'{{col['DefaultValueIfNotNull']}}') AS `{{col['ColumnName']}}`
        ,COALESCE(from_utc_timestamp(to_utc_timestamp(to_timestamp(`{{col['ColumnName']}}`, '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), '{{col['Format']['TimeZone']}}'), "Australia/Melbourne"), CAST('{{col['DefaultValueIfNotNull']}}' as TIMESTAMP)) AS `{{col['ColumnName']}}_Aet`
        {%- endif -%}
        {##}
               {%- elif  col['DataType'] == 'TIMESTAMP' and col['Format']['TimeZone']==None -%}
        {%-if col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_det_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
        ,COALESCE(encrypt_scala_det_binary(cast(from_utc_timestamp(to_timestamp(decrypt_scala_det_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne")as STRING)), encrypt_scala_det_binary(CAST('{{col['DefaultValueIfNotNull']}}' as STRING))) AS `{{col['ColumnName']}}_Aet`
        {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_ndet_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
        ,COALESCE(encrypt_scala_ndet_binary(cast(from_utc_timestamp(to_timestamp(decrypt_scala_ndet_binary(`{{col['ColumnName']}}`), '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne")as STRING)), encrypt_scala_ndet_binary(CAST('{{col['DefaultValueIfNotNull']}}' as STRING))) AS `{{col['ColumnName']}}_Aet`
        {%- else -%}
        ,nvl(`{{col['ColumnName']}}`,'{{col['DefaultValueIfNotNull']}}') AS `{{col['ColumnName']}}`
        ,COALESCE(from_utc_timestamp(to_timestamp(`{{col['ColumnName']}}`, '{{col['Format']['InputFormatString'].replace("'","\\'")}}'), "Australia/Melbourne"), CAST('{{col['DefaultValueIfNotNull']}}' as TIMESTAMP)) AS `{{col['ColumnName']}}_Aet`
        {%- endif -%}
		{##}		
               {%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
		,nvl(`{{col['ColumnName']}}`,'{{col['DefaultValueIfNotNull']}}') AS `{{col['ColumnName']}}`
		,`{{col['ColumnName']}}_Cpy`
		{##}		
               {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'DET'-%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_det_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
		{##}
               {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET'-%}
		,nvl(`{{col['ColumnName']}}`,encrypt_scala_ndet_binary('{{col['DefaultValueIfNotNull']}}')) AS `{{col['ColumnName']}}`
		{##}
                              {%- else -%}
		,nvl(`{{col['ColumnName']}}`,CAST('{{col['DefaultValueIfNotNull']}}' as {{col['DataType']}})) AS `{{col['ColumnName']}}`
		 {##}
               {%- endif -%}
		  	{%- endif -%}  
		  {%- endif -%}{##}
		{%- endfor -%}
		 {##}
		,`Source_Timestamp`
		,`Year_Month`
	FROM  {{template_params['business_unit_name_code']}}_brz_{{template_params['sourceName']|lower}}.{{schema_dict['File']['ObjectName']}}  
	{%- if template_params['load_type']=='T' -%}
	 {##}
	 WHERE `Year_Month` = {{template_params['brz_max_year_month']}}
	 AND `Process_Start_TimeStamp` = (SELECT MAX(`Process_Start_TimeStamp`) FROM  {{template_params['business_unit_name_code']}}_brz_{{template_params['sourceName']|lower}}.{{schema_dict['File']['ObjectName']}} WHERE Year_Month = {{template_params['brz_max_year_month']}})
	{%- elif  template_params['load_type']=='F' -%}
	{##}
	 WHERE `Source_Timestamp` = '{{template_params['full_load_source_timestamp']}}'
	{%- else -%}
	 {##}
	 WHERE `Year_Month` >= {{template_params['Prev_Process_Start_YearMonth']}} 
	 AND `Process_Start_TimeStamp` > '{{template_params['Prev_Process_Start_TimeStamp']}}'
	{%- endif -%}
	) as b);;

ANALYZE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 2: Drop duplicate records
-- *----------------------------------------------*
DELETE FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg WHERE `Dup_Cnt`>1;;

-- *----------------------------------------------*
-- STEP 3: Create/Load existing silver table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}};;
CREATE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}}
	(
	 `Pk_Hash` BINARY
	 ,`Row_Hash` BINARY
	 ,`Effective_Dttm` TIMESTAMP
	 ,`Expiry_Dttm` TIMESTAMP
	 ,`Source_File_Name` STRING
	 ,`Source_App_Name` STRING
	 ,`Record_Type` STRING
	 ,`Record_Insert_Dttm` TIMESTAMP
	 ,`Record_Update_Dttm` TIMESTAMP
	 ,`Process_Instance_Id` STRING
	 ,`Update_Process_Instance_Id` STRING
	 ,`Is_Current` BOOLEAN
	{%- for col in schema_dict['SourceColumns'] -%}
	 {##}
	  {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
		{%- if  col['DataType'] == 'TIMESTAMP' -%}
	,`{{col['ColumnName']}}` {%- if col['IsAttributePII'] == True  -%} BINARY {%- else -%} STRING {%- endif -%} {% if  not(col['Nullable']) %} NOT NULL{% endif %}
	,`{{col['ColumnName']}}_Aet` {%- if col['IsAttributePII'] == True  -%} BINARY {%- else -%}{{col['DataType']}} {%- endif -%} {% if  not(col['Nullable']) %} NOT NULL{% endif %}
	{##}
		{%- elif col['IsAttributePII'] == True and col['EncryptionType']=='FPE' -%}
	,`{{col['ColumnName']}}` STRING{% if  not(col['Nullable']) %} NOT NULL{% endif %}
	,`{{col['ColumnName']}}_Cpy` STRING{% if  not(col['Nullable']) %} NOT NULL{% endif %}
	{##}
		{%- elif col['IsAttributePII'] == True  -%}
	,`{{col['ColumnName']}}` BINARY {% if  not(col['Nullable']) %} NOT NULL{% endif %}
	{##}
		{%- else -%}
	,`{{col['ColumnName']}}` {{col['DataType']}}{% if  not(col['Nullable']) %} NOT NULL{% endif %}
	{##}
		{%- endif -%}  
	  {%- endif -%}
	{%- endfor -%}
	,`Year_Month` INT
)
USING DELTA
PARTITIONED BY (`Year_Month`)
LOCATION '{{template_params['outgoing_path']}}';;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} COMPUTE STATISTICS

-- Now execute load specific template;;
"""
