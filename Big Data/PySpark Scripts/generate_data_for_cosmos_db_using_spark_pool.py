import uuid
import random


columns = ['id', 'ownerId', 'schemaType', 'deviceType', 'macAddress', 'os']
deviceType = ['laptop', 'pc', 'tablet', 'cellphone']
os = ['windows', 'apple', 'android', 'linux']

data_df = spark.createDataFrame([("77_99", 77, 'devices', random.choice(deviceType), uuid.uuid4(), random.choice(os))], columns)

for i in range(100,100000):
    newid = f'77_{i}'
    newRow = spark.createDataFrame([(newid, 77, 'devices', random.choice(deviceType), uuid.uuid4(), random.choice(os))], columns)
    data_df = data_df.union(newRow)

data_df.write.format("cosmos.oltp")\
    .option("spark.synapse.linkedService", "<enter linked service name>")\
    .option("spark.cosmos.container", "<enter container name>")\
    .mode('append')\
    .save()