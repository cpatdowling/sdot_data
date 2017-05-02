import os
import datetime

datapath = "/home/chase/projects/sdot_data/data/parking_data"

infiles = os.listdir(datapath)

#first batch of parallel bulk input ended with 12202015
last = datetime.datetime(year=2011, month=12, day=31)
last = last - datetime.timedelta(days=5)

with open("parallel_bulk.sh", 'w') as f:
    for fname in infiles:
        dt = fname[0:8]
        dt = datetime.datetime.strptime(dt, '%Y%m%d')
        if dt > last:
            print(dt)
            f.write("python /home/chase/projects/sdot_data/src/bulk_load.py " + fname + "\n")
