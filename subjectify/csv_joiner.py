import sys, csv

inf = sys.argv[1]
num = int(sys.argv[2])
outf = sys.argv[3]

head = False
with open(outf, "wb") as out:
    fho = csv.writer(out)
    for x in xrange(1,num+1):
        fn = "%s-%s.csv"%(inf,str(x).rjust(4, "0"))
        print "Reading ", fn
        with open(fn, 'r') as fh:
            r = csv.reader(fh)
            h = r.next()
            if not head:
                fho.writerow(h)
                head = True
            for row in r:
                fho.writerow(row)