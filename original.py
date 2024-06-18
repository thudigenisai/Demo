def gld_delta():
    return r"""
-- *----------------------------------------------*
-- STEP 3.1: Drop duplicate (non soft deleted) worker records which exist as active records in Gold table
-- *----------------------------------------------* 
DELETE FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg
WHERE EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash` AND gld.is_current='true' AND NOT ((gld.`record_type`='D' AND stg.`record_type`<>'D') OR (stg.`record_type`='D' AND gld.`record_type`<>'D')));;
  
-- *----------------------------------------------*
-- STEP 3.2: Create load table by history stitching worker records to insert and Gold records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.{{template_params['worker_table']}}_load;;
CREATE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_load
	AS
	SELECT
		`pk_hash` as `pk_hash`
		,`row_hash` as `row_hash`
		,`effective_dttm` as `effective_dttm`
		,CASE 
		    WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
		    THEN CAST('9999-12-31 00:00:00' as TIMESTAMP)
		    ELSE CAST(COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),NULL)- INTERVAL 1 milliseconds as TIMESTAMP)
		END AS `expiry_dttm`
        ,`source_app_name` as `source_app_name`
		,CASE 
		    WHEN `record_type`<>'D'
		    THEN (
		        CASE
		            WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')=''
		            THEN 'I'
		            ELSE 'U'
		        END
		    )
		    ELSE 'D' 
		END AS `record_type`
		,`record_insert_dttm` as `record_insert_dttm`
		,`record_update_dttm` as `record_update_dttm`
		,`process_instance_id` as `process_instance_id`
		,`update_process_instance_id` as `update_process_instance_id`
		,CASE 
		    WHEN COALESCE((MAX(`effective_dttm`)  OVER(PARTITION BY `pk_hash` ORDER BY `RowCnt` DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)),'')='' 
		    THEN 'true'
		    ELSE 'false'
		END AS `is_current`
		{% for col in schema_dict['target_columns'] %}
		    {% if not col['ignore'] or col['is_primary_key_col'] %}
		        ,`{{col['column_name']}}` as `{{col['column_name']}}`
            {% endif %}
		{% endfor %}
        {% for ik_name in template_params['integration_key_cols_per_name'] %}
            ,`{{ik_name}}` as `{{ik_name}}`
        {% endfor %}
		 ,`year_month` as `year_month`
	FROM
	(
	SELECT ROW_NUMBER() OVER (PARTITION BY `pk_hash` ORDER BY effective_dttm {% if template_params['hist_stitch_sort_on'] %},`{{template_params['hist_stitch_sort_col']}}` {% endif %}) as `RowCnt`,*
	FROM 
		(
			-- ** Identify worker records to insert **
			-- 1) Worker records that exist in Gold table but with different data in source columns
            -- 2) Worker records that exist in Gold table with the same data in source columns, but where duplicate record is not current
            -- 3) Worker records that exist in Gold table with the same data in source columns where duplicte record is current with I or U and worker record is soft delete (or vice-versa)
			(
			SELECT
				stg.`pk_hash` 
				,stg.`row_hash` 
				,stg.`effective_dttm` 
				,stg.`expiry_dttm`
                ,stg.`source_app_name`
				,stg.`record_type`
				,stg.`record_insert_dttm` 
				,stg.`record_update_dttm` 
				,stg.`process_instance_id` 
				,stg.`update_process_instance_id` 
				,stg.`is_current`
				{% for col in schema_dict['target_columns'] %}
                    {% if not col['ignore'] or col['is_primary_key_col'] %} 
                    ,`{{col['column_name']}}` as `{{col['column_name']}}`
                    {% endif %}
                {% endfor %}
                {% for ik_name in template_params['integration_key_cols_per_name'] %}
                    ,`{{ik_name}}` as `{{ik_name}}`
                {% endfor %}
				,stg.`year_month`
			FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg
			WHERE NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash`)
				OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash`)
				AND NOT EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash`)
				)
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash` AND gld.`is_current` = false)
				)
                OR (EXISTS(SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash` AND gld.`is_current` = true AND 
                ((gld.`record_type` = 'D' AND stg.`record_type`<>'D') OR (gld.`record_type` <> 'D' AND stg.`record_type`='D')))
				)
			)
			
			UNION ALL
			-- ** Identify Gold records to update **
            -- 1) Gold records with same pk_hash but different row_hash to any worker record
            -- 2) Gold records with same pk_hash and row_hash as a worker record and Gold record is not a current record
            -- 3) Gold records with same pk_hash and row_hash as a worker record and Gold record is current and soft deleted and worker record is a re-insertion or vice-versa
			(
			SELECT 	
				gld.`pk_hash` as `pk_hash`
				 ,gld.`row_hash` as `row_hash`
				 ,CAST(gld.`effective_dttm` AS TIMESTAMP) as `effective_dttm`
				 ,CAST(gld.`expiry_dttm` AS TIMESTAMP) as `expiry_dttm`
                 ,gld.`source_app_name`
				 ,CASE WHEN gld.`record_type`='D' THEN 'D' ELSE 'U' END as `record_type`
				 ,CAST(gld.`record_insert_dttm` AS TIMESTAMP) as `record_insert_dttm`
				 ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) as `record_update_dttm`
				 ,gld.`process_instance_id` as `process_instance_id`
				 ,'{{template_params['pipeline_run_id']}}' as `update_process_instance_id`
				 ,CAST(0 AS BOOLEAN) as `is_current`
				 {% for col in schema_dict['target_columns'] %}
				     {% if not col['ignore'] or col['is_primary_key_col'] %}
				         {% if col['adb_encryption_type'] in ['DET', 'NDET'] %}
				             ,cast(`{{ col['column_name'] }}` as BINARY) as `{{ col['column_name'] }}`
				         {% else %}
				             ,`{{ col['column_name'] }}` as `{{ col['column_name'] }}`
				         {% endif %}
				     {% endif %}
				 {% endfor %}
                 {% for ik_name in template_params['integration_key_cols_per_name'] %}
                     ,`{{ik_name}}` as `{{ik_name}}`
                 {% endfor %}
				 ,gld.`year_month` as `year_month`
			FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld
			WHERE 	(EXISTS(SELECT 1 FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg WHERE stg.`pk_hash`=gld.`pk_hash`)
				AND NOT EXISTS(SELECT 1 FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg WHERE stg.`pk_hash`=gld.`pk_hash` AND stg.`row_hash`=gld.`row_hash`))
                OR (EXISTS(SELECT 1 FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash` AND gld.`is_current` = false)
				)
                OR (EXISTS(SELECT 1 FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_stg as stg WHERE gld.`pk_hash`=stg.`pk_hash` AND gld.`row_hash`=stg.`row_hash` AND gld.`is_current` = true AND 
                ((gld.`record_type` = 'D' AND stg.`record_type`<>'D') OR (gld.`record_type` <> 'D' AND stg.`record_type`='D')))
				)
			)
		)
	);;

ANALYZE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_load COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 3.3: Delete any records from Gold table with the same pk_hash and row_hash as any record identified in the step above
-- *----------------------------------------------*
DELETE FROM {{template_params['main_database']}}.{{schema_dict['object_name']}} as gld
WHERE EXISTS(SELECT 1 FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_load as `load` WHERE `load`.`pk_hash`=gld.`pk_hash` AND `load`.`row_hash`=gld.`row_hash`);;

"""
