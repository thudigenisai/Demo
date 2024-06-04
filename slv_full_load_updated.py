# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Full Load Jinja2 template

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Silver - Full Load Jinja2 template

# COMMAND ----------

def slv_full():
    return r"""
-- *----------------------------------------------*
-- STEP 4.1: Identify stagings records that will update silver records 
-- 1) staging records that do NOT have same PK_Hash & Row_Hash as current silver record OR staging record is a re-insertion AND staging record is NOT late landing
-- 2) staging records with same Pk_Hash but different Row_Hash to silver record AND staging record has an effective_dttm between silver record effectiveness period (handles late landing)
-- 3) staging records with same Pk_Hash as silver record and silver record is soft deleted AND staging record has an effective_dttm between silver record effectiveness period (handles late landing)
-- *----------------------------------------------*
CREATE OR REPLACE TEMPORARY VIEW staging_records_to_update_silver
    AS
    SELECT stg.`pk_hash` 
    FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg
    WHERE 
        NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash` AND slv.`is_current`=true AND NOT (slv.`record_type`='D' AND stg.`record_type`<>'D')) AND stg.`effective_dttm` > (SELECT max(slv.`effective_dttm`) FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv)
    OR 
        (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND stg.effective_dttm BETWEEN slv.effective_dttm AND slv.expiry_dttm) AND NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash` AND stg.effective_dttm BETWEEN slv.effective_dttm AND slv.expiry_dttm))
    OR 
        EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`record_type`='D' AND slv.`pk_hash`=stg.`pk_hash` AND stg.`effective_dttm` BETWEEN slv.`effective_dttm` AND slv.`expiry_dttm`);;
-- *----------------------------------------------*
-- STEP 4.2: Create load table by history stitching staging records to insert and silver records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;
CREATE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load
    AS
    -- *----------------------------------------------*
    -- STEP 4.2.1: Perform history stitching
    -- *----------------------------------------------*
    SELECT
        `pk_hash` as `pk_hash`
        ,`row_hash` as `row_hash`
        ,`effective_dttm` as `effective_dttm`
        ,CASE WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
            THEN CAST('9999-12-31 00:00:00' as TIMESTAMP)
            ELSE CAST(COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),NULL)- INTERVAL 1 milliseconds as TIMESTAMP)
            END AS `expiry_dttm`
        ,`source_file_name` as `source_file_name`
        ,`source_app_name` as `source_app_name`
        ,CASE WHEN `record_type`<>'D'
            THEN (
            CASE	WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
                THEN 'I'
                ELSE 'U'
                END
            )
            ELSE 'D' END AS `record_type`
        ,`record_insert_dttm` as `record_insert_dttm`
        ,`record_update_dttm` as `record_update_dttm`
        ,`process_instance_id` as `process_instance_id`
        ,`update_process_instance_id` as `update_process_instance_id`
        ,CAST(CASE	WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
                    THEN 1
                    ELSE 0 
                    END as BOOLEAN) AS `is_current`
        {%- for col in schema_dict['SourceColumns'] -%}
        {##}
        {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
            {%- if  col['DataType'] == 'TIMESTAMP' -%}
        ,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
        ,`{{col['ColumnName']}}_Aet` as `{{col['ColumnName']}}_Aet`
        {##}
            {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
        ,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
        ,`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
        {##}
            {%- else -%}
        ,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
        {##}
            {%- endif -%}{##}  
        {%- endif -%}{##}
        {%- endfor -%}
         ,`year_month` as `year_month`	
    FROM
    (
    SELECT ROW_NUMBER() OVER (PARTITION BY pk_hash ORDER BY effective_dttm {%- if template_params['HistStitchSortOn'] -%},{{template_params['HistStitchSortCol']}} {%- endif -%}) as RowCnt,*
    FROM 
        (
            -- *----------------------------------------------*
            -- STEP 4.2.2: Identify silver records to be updated
            -- 1) silver records with same Pk_Hash identified from step 4.1
            -- 2) silver records that have a Pk_Hash that does not exist in staging dataset and max effective_dttm of staging dataset is greater than or equal to silver effective_dttm (soft delete)
            -- *----------------------------------------------*	
            (
            SELECT 
                slv.`pk_hash` as `pk_hash`
                 ,slv.`row_hash` as `row_hash`
                 ,slv.`effective_dttm` as `effective_dttm`
                 ,slv.`expiry_dttm` as `expiry_dttm`
                 ,slv.`source_file_name` as `source_file_name`
                 ,slv.`source_app_name` as `source_app_name`
                 ,slv.`record_type` as `record_type`
                 ,slv.`record_insert_dttm` as `record_insert_dttm`
                 ,CAST('{{template_params['Process_Start_TimeStamp']}}' AS TIMESTAMP) as `record_update_dttm`
                 ,slv.`process_instance_id` as `process_instance_id`
                 ,'{{template_params['pipelineRunID']}}' as `update_process_instance_id`
                 ,CAST(0 AS BOOLEAN) as `is_current`
                {%- for col in schema_dict['SourceColumns'] -%}
                {##}
                {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
                    {%- if  col['DataType'] == 'TIMESTAMP' -%}
                ,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,slv.`{{col['ColumnName']}}_Aet` as `{{col['ColumnName']}}_Aet`
                {##}
                    {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
                ,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,slv.`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
                {##}
                    {%- else -%}
                ,slv.`{{col['ColumnName']}}`
                {##}
                    {%- endif -%}  
                {%- endif -%}{##}
                {%- endfor -%}
                 ,slv.`year_month` as `year_month`
            FROM  {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv
            WHERE (EXISTS(SELECT 1 FROM staging_records_to_update_silver as stg WHERE slv.`pk_hash`=stg.`pk_hash`))
                OR NOT EXISTS (SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE stg.`pk_hash`=slv.`pk_hash`)
                   AND slv.`is_current`=true AND slv.`record_type` <> 'D'
                   AND CAST((SELECT MAX(`effective_dttm`) FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg) as TIMESTAMP) >= CAST(slv.`effective_dttm` as TIMESTAMP) -- to update silver records for soft deletes - ensure that late landing records do not soft delete newer records inserted in silver table
            )
            UNION ALL

            -- *----------------------------------------------*
            -- STEP 4.2.3: Identify new staging records to insert
            -- 1) staging records with a Pk_Hash that does not exist in the silver table
            -- 2) staging records with a Pk_Hash that does exist in the silver table but with a different Row_Hash
            -- 3) staging records with the same Pk_Hash & Row_Hash as a non-current silver record AND the staging effective_dttm is not between the matching silver record effectiveness period
            -- 4) staging records with the same Pk_Hash & Row_Hash as a current silver record and the silver record is soft deleted
            -- *----------------------------------------------*	
            (
            SELECT
                `pk_hash` as `pk_hash`
                 ,`row_hash` as `row_hash`
                 ,`effective_dttm` as `effective_dttm`
                 ,`expiry_dttm` as `expiry_dttm` 
                 ,`source_file_name` as `source_file_name`
                 ,`source_app_name` as `source_app_name`
                 ,`record_type` as `record_type`
                 ,`record_insert_dttm` as `record_insert_dttm`
                 ,`record_update_dttm` as `record_update_dttm`
                 ,`process_instance_id` as `process_instance_id`
                 ,`update_process_instance_id` as `update_process_instance_id`
                 ,`is_current` as `is_current`
                {%- for col in schema_dict['SourceColumns'] -%}
                {##}
                {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
                    {%- if  col['DataType'] == 'TIMESTAMP' -%}
                ,stg.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,stg.`{{col['ColumnName']}}_Aet` as `{{col['ColumnName']}}_Aet`
                {##}
                    {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
                ,stg.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,stg.`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
                {##}
                    {%- else -%}
                ,stg.`{{col['ColumnName']}}`
                {##}
                    {%- endif -%}  
                {%- endif -%}{##}
                {%- endfor -%}
                 ,stg.`year_month` as `year_month`
                 {##}
            FROM  {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg
            WHERE 	NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash`)
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash`)
                    AND NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash`)
                )
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash` AND slv.`is_current` = false AND stg.`effective_dttm` NOT BETWEEN slv.`effective_dttm` AND slv.`expiry_dttm`)
                )
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash` AND slv.`is_current` = true AND (slv.`record_type` = 'D' AND stg.`record_type`<>'D')) AND NOT EXISTS (SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv WHERE slv.`pk_hash`=stg.`pk_hash` AND slv.`row_hash`=stg.`row_hash` AND stg.`effective_dttm` BETWEEN slv.`effective_dttm` AND slv.`expiry_dttm`)
                )
            )
            UNION ALL

            -- *----------------------------------------------*
            -- STEP 4.2.4: Identify silver records for soft deletion
            -- Note: the same records were identified in step 4.2.2 (part 2), but they are re-indentified here to create a new record for the soft deletion
            -- *----------------------------------------------*
            (
            SELECT
                slv.`pk_hash` as `pk_hash`
                 ,slv.`row_hash` as `row_hash`
                 ,CAST((SELECT MAX(`effective_dttm`) FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg) as TIMESTAMP) as `effective_dttm`
                 ,CAST('9999-12-31 00:00:00' as TIMESTAMP) as `expiry_dttm`
                 ,slv.`source_file_name` as `source_file_name`
                 ,slv.`source_app_name` as `source_app_name`
                 ,'D' as `record_type`
                 ,CAST('{{template_params['Process_Start_TimeStamp']}}' AS TIMESTAMP) as `record_insert_dttm`
                 ,CAST(null AS TIMESTAMP) as `record_update_dttm`
                 ,'{{template_params['pipelineRunID']}}' as `process_instance_id`
                 ,CAST(null AS STRING) as `update_process_instance_id`
                 ,CAST(1 as BOOLEAN) as `is_current`
                {%- for col in schema_dict['SourceColumns'] -%}
                {##}
                {%- if  not(col['Ignore']) or col['IsKeyColumn'] -%} 
                    {%- if  col['DataType'] == 'TIMESTAMP' -%}
                ,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,slv.`{{col['ColumnName']}}_Aet` as `{{col['ColumnName']}}_Aet`
                {##}
                    {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
                ,slv.`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
                ,slv.`{{col['ColumnName']}}_Cpy` as `{{col['ColumnName']}}_Cpy`
                {##}
                    {%- else -%}
                ,slv.`{{col['ColumnName']}}`
                {##}
                    {%- endif -%}  
                {%- endif -%}{##}
                {%- endfor -%}
                 ,slv.`year_month` as `year_month`
                 {##}
            FROM  {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv
            WHERE 	slv.`is_current`=true
                AND slv.`record_type` <> 'D'
                AND NOT EXISTS (SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg as stg WHERE stg.`pk_hash`=slv.`pk_hash`)
                AND CAST((SELECT MAX(`effective_dttm`) FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg) as TIMESTAMP) >= CAST(slv.`effective_dttm` as TIMESTAMP) -- ensure that late landing records do not soft delete newer records inserted in silver table
            )
        )
    );;

ANALYZE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 4.3: Delete any records from silver table with the same Pk_Hash & Row_Hash as any record identified in the step above
-- *----------------------------------------------*
DELETE FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} as slv
WHERE 	EXISTS (SELECT 1 FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load as `load` WHERE `load`.`pk_hash`=slv.`pk_hash` AND `load`.`row_hash`=slv.`row_hash`);;
"""
