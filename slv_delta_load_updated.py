# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Delta Load Jinja2 template

# COMMAND ----------

def slv_delta():
  return r"""
-- *----------------------------------------------*
-- STEP 4.1: Drop staging records which exist as active records in silver table
-- *----------------------------------------------* 
DELETE FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg
WHERE EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`Pk_Hash`=stg.`Pk_Hash` AND slv.`Row_Hash`=stg.`Row_Hash` AND slv.Is_Current='true');;
  
-- *----------------------------------------------*
-- STEP 4.2: Identify staging records to insert and silver records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;
CREATE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load
	AS
	SELECT
		`Pk_Hash` as `Pk_Hash`
		,`Row_Hash` as `Row_Hash`
		,`Effective_Dttm` as `Effective_Dttm`
		,CASE 	WHEN COALESCE((MAX(`Effective_Dttm`)  OVER(PARTITION BY `Pk_Hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
		THEN CAST('9999-12-31 00:00:00' as TIMESTAMP)
		ELSE CAST(COALESCE((MAX(`Effective_Dttm`)  OVER(PARTITION BY `Pk_Hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),NULL)- INTERVAL 1 milliseconds as TIMESTAMP)
		END AS `Expiry_Dttm`
		,`Source_File_Name` as `Source_File_Name`
		,`Source_App_Name` as `Source_App_Name`
		,`Record_Type` as `Record_Type`
		,`Record_Insert_Dttm` as `Record_Insert_Dttm`
		,`Record_Update_Dttm` as `Record_Update_Dttm`
		,`Process_Instance_Id` as `Process_Instance_Id`
		,`Update_Process_Instance_Id` as `Update_Process_Instance_Id`
		,CASE  	WHEN COALESCE((MAX(`Effective_Dttm`)  OVER(PARTITION BY `Pk_Hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')='' THEN 'true'
		ELSE 'false'
		END AS `Is_Current`
		{%- for col in schema_dict['SourceColumns'] -%}
		{##}
		{%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
			{%- if  col['DataType'] == 'TIMESTAMP' -%}
		,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
		,`{{col['ColumnName']}}_Aet` as `{{col['ColumnName']}}_Aet`
			{%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
		,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
		,`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
			{%- else -%}
		,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
			{%- endif -%}{##}  
		{%- endif -%}{##}
		{%- endfor -%}
		 {##}
		 ,`Year_Month` as `Year_Month`
	FROM
	(
	SELECT ROW_NUMBER() OVER (PARTITION BY `Pk_Hash` ORDER BY Effective_Dttm {%- if template_params['HistStitchSortOn'] -%},`{{template_params['HistStitchSortCol']}}` {%- endif -%}) as `RowCnt`,*
	FROM 
		(
			-- insert staging records that don't exist in silver table
			-- OR insert staging records that exist in silver table but with different data in source columns
            -- OR insert staging records that exist in silver table with the same data in source columns, but where duplicate record is not current
			(
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
			FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg
			WHERE NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`Pk_Hash`=stg.`Pk_Hash`)
				OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`Pk_Hash`=stg.`Pk_Hash`)
				AND NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`Pk_Hash`=stg.`Pk_Hash` AND slv.`Row_Hash`=stg.`Row_Hash`)
				)
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`Pk_Hash`=stg.`Pk_Hash` AND slv.`Row_Hash`=stg.`Row_Hash` AND slv.`Is_Current` = false)
				)
			)
			
			UNION ALL
			-- update silver records with same Pk_Hash
            -- OR insert staging records that exist in silver table with the same data in source columns, but where duplicate record is not current
			(
			SELECT 	
				slv.`Pk_Hash` as `Pk_Hash`
				 ,slv.`Row_Hash` as `Row_Hash`
				 ,CAST(slv.`Effective_Dttm` AS TIMESTAMP) as `Effective_Dttm`
				 ,CAST(slv.`Expiry_Dttm` AS TIMESTAMP) as `Expiry_Dttm`
				 ,slv.`Source_File_Name` as `Source_File_Name`
				 ,slv.`Source_App_Name` as `Source_App_Name`
				 ,'U' as `Record_Type`
				 ,CAST(slv.`Record_Insert_Dttm` AS TIMESTAMP) as `Record_Insert_Dttm`
				 ,CAST('{{template_params['Process_Start_TimeStamp']}}' AS TIMESTAMP) as `Record_Update_Dttm`
				 ,slv.`Process_Instance_Id` as `Process_Instance_Id`
				 ,'{{template_params['pipelineRunID']}}' as `Update_Process_Instance_Id`
				 ,CAST(0 AS BOOLEAN) as `Is_Current`
				{%- for col in schema_dict['SourceColumns'] -%}
				{##}
				{%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
					{%- if  col['DataType'] == 'TIMESTAMP' -%}
				,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
				,slv.`{{col['ColumnName']}}_Aet` as`{{col['ColumnName']}}_Aet`
					{%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE'-%}
				,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
				,slv.`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
					{%- else -%}
				,slv.`{{col['ColumnName']}}`
					{%- endif -%}  
				{%- endif -%}{##}
				{%- endfor -%}
				{##}
				 ,slv.`Year_Month` as `Year_Month`
			FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv
			WHERE 	(EXISTS(SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE stg.`Pk_Hash`=slv.`Pk_Hash`)
				AND NOT EXISTS(SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE stg.`Pk_Hash`=slv.`Pk_Hash` AND stg.`Row_Hash`=slv.`Row_Hash`))
                OR (EXISTS(SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE slv.`Pk_Hash`=stg.`Pk_Hash` AND slv.`Row_Hash`=stg.`Row_Hash` AND slv.`Is_Current` = false)
				)
			)
		)
	);;

ANALYZE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 4.3: Delete any records from silver table with the same Pk_Hash as any record identified in the step above
-- *----------------------------------------------*
DELETE FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv
WHERE 	EXISTS(SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load as `load` WHERE `load`.`Pk_Hash`=slv.`Pk_Hash`)
	AND EXISTS(SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load as `load` WHERE `load`.`Pk_Hash`=slv.`Pk_Hash` AND `load`.`Row_Hash`=slv.`Row_Hash`);;
"""
