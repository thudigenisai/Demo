# Databricks notebook source
# MAGIC %md
# MAGIC ##### TemplateExecutor class is used across both the bronze & silver layers to execute pattern queries
# MAGIC November 2021

# COMMAND ----------

import delta.exceptions as exc
import random
import time

# COMMAND ----------

class TemplateExecutor():
  '''Class to execute pattern templates'''
  def __init__(self, in_container, out_container, quara_container, layer, lyr_inputs):
    '''Set inputs, schema, ADLS paths and create databases'''
   
    self.inputs = lyr_inputs.inputs
    self.schema_dict = lyr_inputs.schema_dict
    
    self.set_paths(in_container, out_container, quara_container)
    
    self.create_databases(self, layer, self.get_database_schema())
  
  @staticmethod
  def create_databases(self, layer, schema_or_source):
    '''Create database schemas for specified layer'''
    assert layer in ('brz', 'slv', 'gld','plt'), 'Invalid layer specified'
    assert len(self.inputs['business_unit_name_code'])>0 and len(schema_or_source)>0, 'Invalid business_unit_name_code or source_app_name'
    sql('CREATE DATABASE IF NOT EXISTS ' + self.inputs['main_db_prefix'] + self.inputs['business_unit_name_code'].lower() + '_' + layer + '_' + schema_or_source.lower())
    sql('CREATE DATABASE IF NOT EXISTS ' + self.inputs['main_db_prefix'] + self.inputs['business_unit_name_code'].lower() + '_' + layer + '_' + schema_or_source.lower() + '_PiiView')
    sql('CREATE DATABASE IF NOT EXISTS ' + self.inputs['work_db_prefix'] + self.inputs['business_unit_name_code'].lower() + '_work')
  
  @staticmethod
  def generate_adls_path(self, container, schema_or_source, object_name=None):
    '''Return abfss adls path (or dbfs mount path if adls prefix supplied) for specified container'''
    folder_path = schema_or_source
    if object_name is not None:
      folder_path = folder_path + "/" + object_name  # set up folder paths
      
    if len(self.inputs['adls_prefix'])>0:
      return self.inputs['adls_prefix'] + "/mnt/" + container + "/" + folder_path
    return  "abfss://" + container + "@" + self.inputs['storage_account'] + ".dfs.core.windows.net/" + folder_path
  
  @staticmethod  
  def execute_with_retry(self, query):
    retry_sleep = 1  # initial delay time in seconds
    while True:
        try:
            return sql(query)  # try executing sql query
        except (exc.ConcurrentAppendException,
                exc.ConcurrentDeleteReadException,
                exc.ConcurrentDeleteDeleteException,
                exc.MetadataChangedException,
                exc.ConcurrentTransactionException,
                exc.ProtocolChangedException) as e:  # concurrent exception caught
            if retry_sleep <= 600:                   # 10 minute limit on retries
                time.sleep(random.randint(0, retry_sleep))  # exponential backoff with jitter delay
                retry_sleep = retry_sleep * 2
            else:
                raise Exception('Error executing concurrent SQL query after many retries; giving up. {0}'.format(query)) from e
        except BaseException as e:  # some other exception caught, raise exception immediately and do no retry
            raise Exception('Error executing SQL query: {0}'.format(query)) from e
        

  def execute_queries(self, template, template_params, query_delim, log):
    '''Render specified Jinja2 template and execute SQL queries'''   
    queries = Template(template).render(schema_dict = self.schema_dict, template_params = template_params)
    for query in queries.split(query_delim):
      query = query.strip()
      if len(query)>0:
        with log.operation("Execute SQL query", query=query) as exe:
          self.execute_with_retry(self,query)

# COMMAND ----------

class TemplateExecutorBronzeSilver(TemplateExecutor):
  '''Specific functions for broze and silver runs'''
  def get_database_schema(self):
    return self.inputs['source_app_name']
 
  def set_paths(self,in_container, out_container, quara_container):
    '''Generate the ADLS paths'''
    assert len(in_container)>0 and len(out_container)>0 and len(quara_container)>0, 'One or more container names incorrectly specified'
    assert len(self.inputs['source_app_name'])>0, 'Invalid schema name read from ADF'
    assert len(self.inputs['object_name'])>0, 'Invalid object name read from ADF'
    
    self.incoming_path = self.generate_adls_path(self, in_container, self.inputs['source_app_name'], self.inputs['object_name'])
    self.outgoing_path = self.generate_adls_path(self, out_container, self.inputs['source_app_name'], self.inputs['object_name'])
    self.quarantine_path = self.generate_adls_path(self, quara_container, self.inputs['source_app_name'], self.inputs['object_name'])

# COMMAND ----------

class TemplateExecutorGoldPlatinum(TemplateExecutor):
  '''Specific functions for goldPlatinum runs'''
  def get_database_schema(self):
    return self.schema_dict['schema_name']
  
  def set_paths(self,in_container, out_container, quara_container):
    '''Generate the ADLS paths and account for reference load requirements'''
    assert len(in_container)>0 and len(out_container)>0, 'One or more container names incorrectly specified'
    assert len(self.schema_dict['schema_name'])>0, 'Invalid schema name in JSON schema'
    assert len(self.schema_dict['object_name'])>0, 'Invalid object name in JSON schema'
    
    self.merge_table_path = self.generate_adls_path(self, "wip", self.inputs['Layer']+"_merge/" + self.schema_dict['schema_name'], self.schema_dict['object_name']) + "/" + self.inputs['pipeline_run_id'] + "/" +str(int(time.time()))
    
    if self.inputs['load_type'] == 'R':
      self.outgoing_path = self.generate_adls_path(self, out_container, self.schema_dict['schema_name'])
    else:
      assert len(self.schema_dict['source_application_name'])>0 , 'Invalid source application name in JSON schema'
      self.outgoing_path = self.generate_adls_path(self, out_container, self.schema_dict['schema_name'], self.schema_dict['object_name'])
