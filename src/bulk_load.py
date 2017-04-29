from elasticsearch import Elasticsearch
import os
import csv
import sys
from datetime import *

ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = "sdotparking"
TYPE_NAME = "transaction"
ID_FIELD = "dataid"

headerconfig = {
        "dataid": {"type": "integer"},
        "metercode": {"type": "integer"},
        "transactionid": {"type": "integer"},
        "transactiondatetime": {"type": "datetime"},
        "amount": {"type": "float"},
        "usernumber": {"type": "integer"},
        "paymentmean": {"type": "string"},
        "paidduration": {"type": "integer"},
        "elementkey": {"type": "integer"},
        "transactionyear": {"type": "year"},
        "transactionmonth": {"type": "month"},
        "vendor": {"type": "string"}
    }


es = Elasticsearch(hosts = [ES_HOST])


"""
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
"""

datapath = "/home/chase/projects/sdot_data/data/parking_data"
fname = sys.argv[1]

with open(datapath + "/" + fname, 'r') as f:
    header = f.readline().strip().split(",")
    header = [ token.lower() for token in header ]
    data = [ token.strip().split(",") for token in f.readlines() ]

bulk_data = []
for row in data:
    data_dict = {}
    for i in range(len(row)):
        try:
            if headerconfig[header[i]]["type"] == "integer":
                data_dict[header[i]] = int(row[i])
            elif headerconfig[header[i]]["type"] == "datetime":
                date_object = datetime.strptime(row[i], '%m/%d/%Y %H:%M:%S')
                data_dict[header[i]] = date_object
            elif headerconfig[header[i]]["type"] == "year":
                pass
            elif headerconfig[header[i]]["type"] == "month":
                pass
            else:
                data_dict[header[i]] = row[i]
        except:
            data_dict[header[i]] = row[i]
    op_dict = {
        "index": {
            "_index": INDEX_NAME,
            "_type": TYPE_NAME,
            "_id": data_dict[ID_FIELD]
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

#chunk bulks
numchunks = len(bulk_data) / 100
chunks = []
for i in range(numchunks):
    chunks.append(bulk_data[i*100:(i+1)*100])
chunks.append(bulk_data[((numchunks+1)*100 + 1):len(bulk_data)-1])
print("bulk indexing " + fname)
print(str(len(bulk_data)) + " records")
for j in range(len(chunks)):
    if len(chunks[j]) > 0:
        res = es.bulk(index = INDEX_NAME, body = chunks[j], refresh = True)
