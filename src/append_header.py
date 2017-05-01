import os
import datetime

files = os.listdir("../data")

if not os.path.exists("../data/parking_data"):
    os.mkdir("../data/parking_data")

header = ["DataId", "MeterCode", "TransactionId", "TransactionDateTime",
          "Amount", "UserNumber", "PaymentMean", "PaidDuration", "ElementKey",
          "TransactionYear", "TransactionMonth", "Vendor"]

for fname in files:
    if fname[0] != "p":
        with open("../data/" + fname, 'r') as f:
            data = f.readlines()

            dates = fname.split("-")
            d1 = datetime.datetime.strptime(dates[0], '%m%d%Y')
            d2 = datetime.datetime.strptime(dates[1].rstrip(".csv"), '%m%d%Y')

            d1 = datetime.datetime.strftime(d1, '%Y%m%d')
            d2 = datetime.datetime.strftime(d2, '%Y%m%d')
            fname = d1 + "-" + d2 + ".csv"

            outfile = open("../data/parking_data/" + fname, 'w')
            outfile.write(",".join(header) + "\n")
            outfile.write("".join(data))
            outfile.close()
