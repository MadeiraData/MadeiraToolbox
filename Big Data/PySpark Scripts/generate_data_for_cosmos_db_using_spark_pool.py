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

num_rows = 100

data_df = pd.DataFrame(np.random.randint(0,100,size=(10, 1)), columns=['id'])

data_df["deviceType"] = np.random.choice(deviceType, size=len(data_df))
data_df['ownerId'] = 77
data_df['schemaType'] = 'devices'
data_df['os']= np.random.choice(os, size=len(data_df))
data_df['macAddress'] = df.apply(lambda _: f'{uuid.uuid4()}', axis=1)


display(data_df)

data_df.write.format("cosmos.oltp")\
    .option("spark.synapse.linkedService", "<enter linked service name>")\
    .option("spark.cosmos.container", "<enter container name>")\
    .mode('append')\
    .save()