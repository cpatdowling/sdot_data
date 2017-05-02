import sys
import os
import numpy as np
import urllib
import csv
import urllib2
from datetime import *
from elasticsearch import Elasticsearch

#failure email
sys.path.append("/home/chase")
from send_failure_email import *

#global settings
daysback = 1
datapath = "/home/chase/projects/sdot_data/data"
url1='http://web6.seattle.gov/SDOT/wapiParkingStudy/api/ParkingTransaction?from='
url2='&to='

###Poll SDOT parking data API####
try:
    now = datetime.today()
    now_str = now.strftime('%m%d%Y')

    past = now - timedelta(days=daysback)
    past_str = past.strftime('%m%d%Y')

    req_url = url1 + past_str + url2 + now_str
    response = urllib2.urlopen(req_url).read()
    res=response.split('\r\n')
    result = [ re.split(',') for re in res ]

    fname=past_str+'-'+now_str+'.csv'
    fi=open(datapath + "/" + fname,'wb')
    wr=csv.writer(fi)
    for row in result:
        wr.writerow(row)
    fi.close()

    #add header to file in parking_data directory, I hate csv writer, too lazy

    d1 = datetime.datetime.strptime(dates[0], '%m%d%Y')
    d2 = datetime.datetime.strptime(dates[1].rstrip(".csv"), '%m%d%Y')

    d1 = datetime.datetime.strftime(d1, '%Y%m%d')
    d2 = datetime.datetime.strftime(d2, '%Y%m%d')
    fname = d1 + "-" + d2 + ".csv"

    outfile = open("../data/parking_data/" + fname, 'w')

    header = ["DataId", "MeterCode", "TransactionId", "TransactionDateTime",
          "Amount", "UserNumber", "PaymentMean", "PaidDuration", "ElementKey",
          "TransactionYear", "TransactionMonth", "Vendor"]
    with open(datapath + "/" + fname, 'r') as f:
        data = f.readlines()
    with open(datapath + "/parking_data/" + fname, 'w') as outfile:
        outfile.write(",".join(header) + "\n")
        outfile.write("".join(data))
        

except Exception as err:
    fail = failure("SDOT API query failure")
    fail.send_message(str(err))
    sys.exit()

###Index data in elasticsearch###
ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = "sdotparking"
TYPE_NAME = "transaction"
ID_FIELD = "dataid"

es = Elasticsearch(hosts = [ES_HOST])

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

datapath = "/home/chase/projects/sdot_data/data/parking_data/"

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

try:
    chunks = [ bulk_data[i:i+500] for i in xrange(0, len(bulk_data), 500) ]
    for j in range(len(chunks)):
        if len(chunks[j]) > 0:
            res = es.bulk(index = INDEX_NAME, body = chunks[j], refresh = True)
except Exception as err:
    fail = failure("SDOT data local indexing failure")
    fail.send_message(str(err))
    sys.exit()
