import os

datapath = "/home/chase/projects/sdot_data/data/parking_data"

infiles = os.listdir(datapath)

#first batch of parallel bulk input ended with 12202015

with open("parallel_bulk.sh", 'w') as f:
    for fname in infiles:
        f.write("python /home/chase/projects/sdot_data/src/bulk_load.py " + fname + "\n")
