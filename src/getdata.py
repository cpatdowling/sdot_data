''' Python code to pull down sdot data
'''

import numpy as np
import urllib
import csv
import urllib2
import datetime as dt
from datetime import datetime
import time


url1='http://web6.seattle.gov/SDOT/wapiParkingStudy/api/ParkingTransaction?from='
url2='&to='

#sd='01012013' first date
sd = '02182017' #picking up from here
sd1=sd

numfiles = 1 #500
totaldays = 70 #up to April 28, 2017

ds = 3

interval = int(int(totaldays/ds)/numfiles)
print(interval)
sd_=datetime.strptime(sd,'%m%d%Y')
for j in range(numfiles):
    result=[]
    start = j*interval
    end = (j+1)*interval
    for i in range(start,end):
        ed_=sd_+dt.timedelta(days=ds)
        ed=ed_.strftime('%m%d%Y')
        print("retrieving " + str(ed))
        url=url1+sd+url2+ed
        response = urllib2.urlopen(url).read()
        res=response.split('\r\n')
        res=res[:-1]
        if i!=0:
            res=res[1:]
        print( str(len(res)) + " transactions")
        result = [ re.split(',') for re in res ]
        sd=ed
        sd_=ed_
        time.sleep(10)

    t1 = (ed_ - dt.timedelta(days=ds*(end - start))).strftime('%m%d%Y')
    t2 = ed
    name=t1+'-'+t2+'.csv'
    fi=open("../data/" + name,'wb')
    wr=csv.writer(fi)
    for row in result:
        wr.writerow(row)
