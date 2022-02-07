imort pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime


host = 'https://...'
key = 'yourkey'


options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 10

database = 'my_cosmos_database'
container =  "my_container"

dbLink = 'dbs/' + database
collLink = dbLink + '/colls/' + container

# define the query to get all documents that you want to change
querystr = "SELECT * FROM c where c.Documentid <> c.pk"

connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery

client = document_client.DocumentClient(host, {'masterKey': key}, connectionPolicy)

query = client.QueryDocuments(collLink, querystr, options=options, partition_key=None)

# here in each document found in the above query I am adding
# a new keyword to each document "new_property"
for document in query:
      options = {}
      options['partitionKey'] = document["pk"]
      client.DeleteDocument(f"""{collLink}/docs/{document['id']}""", options)