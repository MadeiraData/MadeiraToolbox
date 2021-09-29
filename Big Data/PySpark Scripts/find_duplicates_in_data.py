# This script will load the parquet files from folder on hdfs or s3 or any other path that is accessable
# Files can also be of csv or json format, then need to change format parameter

s3_path = 's3://bucket/folder_holding_the_files'

data = spark.read.option("recursiveFileLookup", "true").load(s3_path,format="parquet")

duplicates = data \
    .groupby(data.columns) \
    .count() \
    .where('count > 1')

if duplicates.count() > 0:
  display(duplicates.sort('count', ascending=False) )
else:
  print ("There were no duplicates found in data")


