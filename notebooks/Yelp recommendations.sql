-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #Analyzing the Yelp Academic Dataset
-- MAGIC 
-- MAGIC ###Objective
-- MAGIC Use the Yelp database to try the key steps in performing data analytics using Databricks. 
-- MAGIC 
-- MAGIC ###Source
-- MAGIC [Yelp Dataset](https://www.yelp.com/dataset/challenge)
-- MAGIC 
-- MAGIC ###Documentation 
-- MAGIC [Yelp Dataset Documentation](https://www.yelp.com/dataset/documentation/main) can be found here. 
-- MAGIC We can also get some ideas of types of analysis from [here](https://drill.apache.org/docs/analyzing-the-yelp-academic-dataset/). We also use [this class project guidelines](ftp://ftp.ecs.csus.edu/nguyendh/CSC%20230%20Projects/Data%20Analytics%20using%20Yelp%20Data.pdf) to get some functional requirements for the project to make this exercise a little bit more realistic. 
-- MAGIC 
-- MAGIC ###Note for me
-- MAGIC 1. [Model for fake reviews](https://medium.com/@zhiwei_zhang/final-blog-642fb9c7e781)
-- MAGIC 2. Correlate public datasets such as income etc. with the zip codes to see if there is any trend in types of resturants and count of feedback by income and other demographics. 
-- MAGIC 3. [Steps to setup databricks CLI](https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html)
-- MAGIC 4. [Command to create secret scope](https://social.msdn.microsoft.com/Forums/azure/en-US/3783dba6-0e3c-48c7-8c72-4e275a215d57/solved-databricks-create-secret-scopes?forum=AzureDataLake)
-- MAGIC 5. [Secrets workflow example](https://docs.azuredatabricks.net/user-guide/secrets/example-secret-workflow.html#create-the-secrets-in-an-azure-key-vault-backed-scope)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC List the files in the blob store. 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val storage_account_name = "justanalytics"
-- MAGIC val container_name = "yelp-reviews"
-- MAGIC 
-- MAGIC /* sample code from https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html */
-- MAGIC spark.conf.set(
-- MAGIC    "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
-- MAGIC    dbutils.secrets.get(scope = "yelp-reviews-store", key = "storage_account_access_key")
-- MAGIC )
-- MAGIC 
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.sas."+container_name+"."+storage_account_name+".blob.core.windows.net",
-- MAGIC   dbutils.secrets.get(scope = "yelp-reviews-store", key = "sas_token")
-- MAGIC )
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Mount the container as /mnt/yelp-reviews into the DBFS.

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC 
-- MAGIC val mount_name = "yelp-reviews"
-- MAGIC 
-- MAGIC /* unmount the filesystem if already mounted */
-- MAGIC dbutils.fs.unmount("/mnt/" + mount_name)
-- MAGIC 
-- MAGIC /*https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html*/
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net/",
-- MAGIC   mountPoint = "/mnt/"+ mount_name,
-- MAGIC   extraConfigs = Map("fs.azure.account.key.justanalytics.blob.core.windows.net"->dbutils.secrets.get(scope = "yelp-reviews-store", key = "storage_account_access_key")))
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("/mnt/"+ mount_name))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Read the JSON file as a dataframe and display the contents. 

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC 
-- MAGIC val file_type = "JSON"
-- MAGIC val file_location = "/mnt/yelp-reviews/business-reduced.json" 
-- MAGIC val df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
-- MAGIC display(df)
-- MAGIC df.printSchema