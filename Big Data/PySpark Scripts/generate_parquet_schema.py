# Sometimes you get a bunch of Parquet files and when trying to read on folder level,
# you get error message: "Unable to infer schema for parquet. It must be specified manually."
# There can be multiple reasons : not all files in the data folder have the same structure, maybe many of them are empty
# or some cna be corrupted

# Here is how you you can generate the parquet schema

df = spark.read.parquet('s3://bucketname/datafiles')

# First part is to find one parquet file that you can read and that has some data.
# Then you can iterate over its columns to build the statement that will describe columns and their datatypes

df = spark.read.parquet('s3://bucketname/datafiles/NewReport-00001.snappy.parquet')

# we will use the following data types definitions,
# this version of the script does not have nested structure generation yet! only basic datatypes

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,BooleanType,BinaryType,LongType,DoubleType

# this function will convert the data type names to proper definition
def switcher(col_type):
  converter={
             'string': 'StringType()',
             'double': 'DoubleType()',
             'timestamp': 'TimestampType()',
             'integer': 'IntegerType()',
             'boolean': 'BooleanType()',
             'binary': 'BinaryType()',
             'long': 'LongType()'
             }
  return converter.get(col_type,"Invalid column type!")

# Here is a script that generates the Struct file:

schema_definition = 'FileSchema = StructType(['
for col in df.dtypes:
    schema_definition = '{} \n {}'.format(schema_definition,
                                          '''StructField('{}', {}, True),'''.format(col[0], switcher(col[1])))

schema_definition = '{} \n ])'.format(schema_definition)

print(schema_definition)

exec(schema_definition)

#Here is an example on how FileSchema will look like:
FileSchema = StructType([
 StructField('id', IntegerType(), True),
 StructField('identity', StringType(), True),
 StructField('start_date', TimestampType(), True), # this last ',' does not produce any error, no need to remove it
])

#Now I can read the data on the folder level

df = spark.read.schema(FileSchema).parquet('s3://bucketname/datafiles/')

display(df)

# if you still have issues with the files that are corrupted or have different extension, you can use this command to ignore them

spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

# as another workaround, you can set to read only files with specific format or set path pattern

df = spark.read.load('s3://bucketname/datafiles/',
                     format="parquet", pathGlobFilter="*.parquet")


# if there is a complicated folders structure, you can set RecursiveFileLookup on

df = spark.read.schema(FileSchema)\
 .option("recursiveFileLookup", "true")\
 .parquet('s3://bucketname/datafiles/')
