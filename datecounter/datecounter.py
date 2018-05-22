""" datecounter.py
A small utility to return a sorted list of file modification dates along with counts.
Usage: datecounter.py [directory]
If no directory is supplied, the current path will be used.
"""

import os, sys
from datetime import datetime

results = {}
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        dirpath = sys.argv[1]
    else:
        sys.exit("Directory does not exist")
else:
    dirpath = os.getcwd()

for f in os.listdir(dirpath):
    fpath = os.path.join(dirpath, f)
    dt = datetime.fromtimestamp(os.path.getmtime(fpath)).date()
    if dt in results.keys():
        results[dt] = results[dt] + 1
    else:
        results[dt] = 1

for result, val in sorted(results.iteritems()):
    print result.strftime("%a %d %b %Y"), ":", val
