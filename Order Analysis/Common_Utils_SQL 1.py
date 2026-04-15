# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Notebook Details
# MAGIC > Notebook_name: Common_Utils
# MAGIC
# MAGIC > Script_Author: jrobertr@its.jnj.com
# MAGIC
# MAGIC > Create_date: 25 Aug 2022
# MAGIC
# MAGIC **Changelog:**
# MAGIC - 25/08/2022 v1.0: AFOE-XXXX Implemented first version of this notebook

# COMMAND ----------

platform_config = {
    'client_id': dbutils.secrets.get(scope="jjvmffhuv_scope",key="jjvmffhuv_client"),
    'client_key': dbutils.secrets.get(scope="jjvmffhuv_scope",key="jjvmffhuv_key"),
    'tenant_id': dbutils.secrets.get(scope="jjvmffhuv_scope",key="tenant_id"),
    'landing_storage_acc': dbutils.secrets.get(scope="jjvmffhuv_scope",key="landing_storage_acc"),
    'landing_container': dbutils.secrets.get(scope="jjvmffhuv_scope",key="landing_container"),
    'sql_jdbc_url': dbutils.secrets.get(scope="jjvmffhuv_scope",key="sql_jdbc_url")
  }

# COMMAND ----------

url = ""
for char in platform_config["sql_jdbc_url"]:
  print(char, end =" ")

# COMMAND ----------

def initialize_storage_params(platform_config):
  # TODO - this should probably be configurable by job/environment
  spark.conf.set("spark.sql.shuffle.partitions", 20)

  # Defining the service principal credentials for the Azure storage account
  # -------------- COMMMENTING THESE OUT AS THE TOKENS HAVE EXPIRED. WILL USE ACCESS TOKENS IN THE MEANTIME AS A TEMPORARY MEASURE
  # spark.conf.set("fs.azure.account.auth.type", "OAuth")
  # spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  # spark.conf.set("fs.azure.account.oauth2.client.id", platform_config['client_id'])
  # spark.conf.set("fs.azure.account.oauth2.client.secret", platform_config['client_key'])
  # spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{}/oauth2/token".format(platform_config['tenant_id']))
  # -------------------------
  #spark.conf.set(f"fs.azure.account.key.{platform_config['staging_storage_acc']}.blob.core.windows.net", platform_config['staging_storage_access_key'])
  # ----------------- USING ACCESS TOKEN BELOW (TEMPORARY)
  spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(platform_config["landing_storage_acc"]), dbutils.secrets.get(scope="jjvmffhuv_scope", key="landing_key"))

  # ---------------------------
  spark.conf.set("spark.databricks.io.cache.enabled", True)
                 
  spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
  spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)

# COMMAND ----------

# TODO Seperate Deltalake and Synapse and load only required elements

# Deltalake Utils Functions

# this should only run the first time this notebook is run on a new environment
def is_first_run_delta(table_location):
  is_delta_table = DeltaTable.isDeltaTable(spark, table_location)
  
  if not is_delta_table:
    # do a further check on the filesystem before we decide to overwrite the table
    print('not a delta table - check filesystem before overwriting...')
    return not delta_log_folder_exists(table_location)
  else:
    return False

# check if _delta_log folder exists for historian_tag_data using adls library
# to be sure there is no data here before overwriting the content
def delta_log_folder_exists(table_location):
  try:  
    credential = ClientSecretCredential(platform_config['tenant_id'], platform_config['client_id'], platform_config['client_key'])
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", platform_config['landing_storage_acc']), credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=platform_config['landing_container'])
    directory_client = file_system_client.get_directory_client(table_location+'/_delta_log')
    return directory_client.exists() 
  except Exception as e:
    # Failed to determine if directory exists or not - so bail out and throw exception
    print('exception', e)
    raise Exception("Unknown if _delta_log directory exists")
  finally:
    file_system_client.close()
    service_client.close()  
    
def register_delta_table(table_location, table_name):
  spark.sql("""
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
  """.format(table_name, table_location))

# Clean up Deltalake table
def cleanup_deltalake(base_table, table_location = ''):
  spark.sql("DROP TABLE IF EXISTS {}".format(base_table))
  if table_location:
    dbutils.fs.rm(table_location,recurse=True)
  


# COMMAND ----------

# SQL Utils Functions

# utility function to check if this table needs to be created
def is_first_run_target(config, target_table):
  schema_name, table_name = target_table.split('.')
  
  query = """
    SELECT case 
       when '{schema}.{table}' in (select TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME='{table}' and TABLE_SCHEMA='{schema}') 
       then 0
       else 1
    end as first_run
  """.format(schema=schema_name, table=table_name)
  
  df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .option("url", config['sql_jdbc_url']) \
  .option("query", query) \
  .load()
  
  return bool(df.collect()[0][0])
  
# utility function to write data to SQL DB - use pre_actions param to create destination table if required  
def copy_to_target(pipeline_id, config, source_df, table, pk_fields, pre_actions=''):
  schema_name, table_name = table.split('.')
  upsert_schema_name = 'upsert_temp'
  upsert_table_name = '{}_{}_upsert_temp'.format(table_name, pipeline_id)
  upsert_table = f"{upsert_schema_name}.{upsert_table_name}"
  merge_query = "exec dbo.Merge_Tables_CS '{}', '{}', '{}', '{}', '{}';".format(table_name, upsert_table_name, schema_name, upsert_schema_name, pk_fields.replace("'", "''"));
  print("Merge query : " + merge_query)
  
  (source_df.write
    #.format("com.microsoft.sqlserver.jdbc.spark") \
    .format("jdbc")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .option("url", config['sql_jdbc_url']) \
    .option("dbTable", upsert_table) \
    .save())
  print("Upsert Table written to target. Now performing MERGE.")
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(config['sql_jdbc_url'])
  exec_statement = con.prepareCall(merge_query)
  for x in range(5):
    try:
      exec_statement.execute()
      print("Merge command executed on table : " + table)
      break
    except Exception as e:
      deadlocked = "deadlocked on lock" in str(e)
      if deadlocked:
        print("Deadlock occured. Retrying merge.")
        continue
      else:
        raise #
  exec_statement.close()
  con.close()

def copy_to_target_retry(pipeline_id, config, source_df, target_table, pk_fields, retry_count, try_list, pre_actions=''):
  if pre_actions:
    print("Pre Action - Table creation required.")
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(config['sql_jdbc_url'])
    exec_statement = con.prepareCall(pre_actions)
    exec_statement.execute()
    print("Pre Action query executed")
    exec_statement.close()
    con.close()
  for x in range(int(retry_count)):
      try:
          copy_to_target(pipeline_id, config, source_df, target_table, pk_fields)
          print('copy_to_target success in try ' + str(x))
          break
      except Exception as e:
          should_retry = any(value in str(e) for value in try_list)
          if should_retry:
              print("copy_to_target retry",x)
              continue
          else:
              print("no matching error in")
              print(str(e))
              log_merge_dupes(e, pk_fields)
              sys.exit()

def log_merge_dupes(err, pk_fields):
    if 'MERGE statement attempted to UPDATE or DELETE the same row more than once' in str(err):
        p_key=pk_fields.replace("'","")
        spark.sql("select {p_key},count(*) from output_results group by {p_key} having count(*) > 1".format(p_key=p_key)).show()
    else:
        print(err)
        
        
def cleanup_target(config, tables):
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(config['sql_jdbc_url'])
  drop_tables_list = list(map(lambda table: "IF OBJECT_ID('{}') IS NOT NULL BEGIN DROP TABLE {} END;".format(table, table), tables))
  for drop_table in drop_tables_list:
    drop_table_query = ''.join(drop_table)
    print("query 1 : " + drop_table_query)
    exec_statement = con.prepareCall(drop_table_query)
    exec_statement.execute()
  exec_statement.close()
  con.close()
  
    

# COMMAND ----------

def query_target(config, query):
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(config['sql_jdbc_url'])
  exec_statement = con.prepareCall(query)
  exec_statement.execute()
  exec_statement.close()
  con.close()

# COMMAND ----------

def get_target_df(config, query):
  return spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("url", config['sql_jdbc_url']) \
    .option("query", query) \
    .load()

# COMMAND ----------

initialize_storage_params(platform_config)
