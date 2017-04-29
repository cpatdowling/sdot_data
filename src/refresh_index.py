from elasticsearch import Elasticsearch
import os
import csv
import sys
from datetime import *

ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = "sdotparking"
TYPE_NAME = "transaction"
ID_FIELD = "dataid"

es = Elasticsearch(hosts = [ES_HOST])


#refresh index
if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))

#running locally, so use one shard and no replicas
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

