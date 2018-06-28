import sys, csv

infile = sys.argv[1]

with open(infile, "r") as f_in:
    reader = csv.reader(f_in)
    header = reader.next()
    count = 0
    outnum = 1
    batch = []
    for row in reader:
        batch.append(row)
        count += 1
        if count == 5000:
            outfile = "%s-batched-%s.csv" % (infile[:-4], str(outnum).rjust(4, "0"))
            with open(outfile, "wb") as f_out:
                writer = csv.writer(f_out)
                writer.writerow(header)
                writer.writerows(batch)
            batch = []
            count = 0
            outnum += 1

outfile = "%s-batched-%s.csv" % (infile[:-4], str(outnum).rjust(4, "0"))
with open(outfile, "wb") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(header)
    writer.writerows(batch)
