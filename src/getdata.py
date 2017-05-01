''' Python code to pull down sdot data
'''

import numpy as np
import urllib
import csv
import urllib2
import datetime as dt
from datetime import datetime
import time
import sys


url1='http://web6.seattle.gov/SDOT/wapiParkingStudy/api/ParkingTransaction?from='
url2='&to='

#sdstr='01012012' #first date
sdstr=sys.argv[1]
sd = datetime.strptime(sdstr,'%m%d%Y')

lastday = datetime(2017, 4, 30) #up to April 30, 2017

totaldays = (lastday - sd).days

ds = 1

for j in range(totaldays):
    result=[]
    print("retrieving " + str(sd))
    ed = sd + dt.timedelta(days=ds)
    edstr = ed.strftime('%m%d%Y')
    url=url1+sdstr+url2+edstr
    response = urllib2.urlopen(url).read()
    res=response.split('\r\n')
    print( str(len(res)) + " transactions")
    result = [ re.split(',') for re in res ]
    
    name=sdstr+'-'+edstr+'.csv'
    fi=open("../data/" + name,'wb')
    wr=csv.writer(fi)
    for row in result:
        wr.writerow(row)

    sd=ed
    sdstr=edstr
    time.sleep(5)
