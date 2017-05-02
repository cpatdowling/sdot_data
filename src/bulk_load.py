from elasticsearch import Elasticsearch
import os
import csv
import sys
from datetime import *

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

ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = "sdotparking"
TYPE_NAME = "transaction"
ID_FIELD = "dataid"

es_cred_file = open("/home/chase/.escreds", 'r'))
user = es_cred_file.readline().strip()
pswd = es_cred_file.readline().strip()

es = Elasticsearch(hosts = [ES_HOST], http_auth=(user, paswd))


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

#chunk bulk
chunks = [ bulk_data[i:i+500] for i in xrange(0, len(bulk_data), 500) ]
for j in range(len(chunks)):
    if len(chunks[j]) > 0:
        res = es.bulk(index = INDEX_NAME, body = chunks[j], refresh = True)
