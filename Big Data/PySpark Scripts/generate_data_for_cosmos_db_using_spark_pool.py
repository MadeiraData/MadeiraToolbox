# the below script is generating documents for CosmosDB container
# document looks as following
# {
#    "id": "77_912",    #this field is a unique id
#    "ownerId": 77,     #this field is used as a partitioning key
#    "schemaType": "devices",
#    "deviceType": "laptop",
#    "macAddress": "23007a78-0000-0100-0000-621887c80000",
#    "os": "windows",
# }

import uuid
import random
import numpy as np


columns = ['id', 'ownerId', 'schemaType','deviceType','macAddress','os']
deviceType = ['laptop','pc','tablet','cellphone']
os = ['windows','apple','android','linux']

data_df = spark.createDataFrame([(
                f'77_{np.random.randint(99,100000,size=(10, 1))}',
                77,
                'devices',
                random.choice(deviceType),
                f'{uuid.uuid4()}',
                random.choice(os))], columns)

data_df.write.format("cosmos.oltp")\
    .option("spark.synapse.linkedService", "<enter linked service name>")\
    .option("spark.cosmos.container", "<enter container name>")\
    .mode('append')\
    .save()