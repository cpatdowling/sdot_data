import os

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

            outfile = open("../data/parking_data/" + fname, 'w')
            outfile.write(",".join(header) + "\n")
            outfile.write("".join(data))
            outfile.close()
