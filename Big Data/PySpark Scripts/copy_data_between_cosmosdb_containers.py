# This code was written for Databricks Notebook
# Dependency: install this package from Maven, coordinates: com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.2.0

#Azure Cosmos DB account -> Settings -> Keys -> URI
host = 'https://***:443/'
#Azure Cosmos DB account -> Settings -> Keys -> Primary/secondary key
key = '****'
database = 'putherecosmosdbdatabase'
sourceContainer = "container_source"
targetContainer = "container_target"

cfg = {
  "spark.cosmos.accountEndpoint" : host,
  "spark.cosmos.accountKey" : key,
  "spark.cosmos.database" : database,
  "spark.cosmos.container" : sourceContainer,
}

cfg_target = {
  "spark.cosmos.accountEndpoint" : host,
  "spark.cosmos.accountKey" : key,
  "spark.cosmos.database" : database,
  "spark.cosmos.container" : targetContainer,
}

spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", host)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", key)

import uuid

from pyspark.sql.functions import *

# this query loads the data from the source container, can filter the data on any property, aggregate it etc.
query = """select uuid() as u1,*
from cosmosCatalog.{}.{} 
where columnA = "some_value"
limit 5 """.format(database, sourceContainer)

df = spark.sql(query)

display(df)

#in this code snippet I am generating a new column pk for compound partitioning key
# I will not take all properties from the source document, only subset
df.withColumn("pk",concat_ws("_",col("propertyA"),col("propertyB")))\
      .withColumn("OldDocumentId",col("id"))\
      .withColumn("id",col("u1"))\
      ["id","pk","OldDocumentId","propertyC","propertyD"]\
      .write\
         .format("cosmos.oltp")\
         .options(**cfg_target)\
         .mode("APPEND")\
         .save()