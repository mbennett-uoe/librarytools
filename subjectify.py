#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""subjectify.py: A tool to retrieve DDC/LCC identifiers from OCLC's Classify API

Version: 1.5
Author: Mike Bennett <mike.bennett@ed.ac.uk>

Python library requirements: requests

Subjectify takes a CSV containing ISBN/ISSNs, and optionally Author/Title data and
performs a series of lookups against the OCLC Classify2 API to retrieve Dewey Decimal
and Library of Congress subject classifiers for each item, writing the results to a
new CSV file.

Usage: 'subjectify.py infile.csv outfile.csv'
"""

import sys, os, csv, time, argparse  # standard python libs
import xml.etree.ElementTree as ET  # standard python libs
import requests  # external dependency

endpoint_url = "http://classify.oclc.org/classify2/Classify"  # OCLC Classify API URL
base_querystring = "?summary=true&maxRecs=1"
ns = {"classify": "http://classify.oclc.org"}  # xml namespace
default_fields = ["isbn", "issn", "author", "title"]  # default csv fields
verbose = False  # was program started with -v?
exact_searches = True  # exact match flag


def load_data(infile, fields="default", skipheader = False):
    """Read a CSV file and return a list of rows"""
    # Make sure file exists
    if not os.path.isfile(infile):
        sys.exit("Fatal Error: Input file does not exist!")
    # Attempt to open and read file
    try:
        with open(infile, "r") as csvfile:
            if fields == "file":
                reader = csv.DictReader(csvfile)
            elif fields == "default":
                reader = csv.DictReader(csvfile, fieldnames=default_fields)
            elif fields == "none":
                reader = csv.reader(csvfile)
            records_in = []
            for row in reader:
                records_in.append(row)
        if skipheader:
            records_in = records_in[1:]

        return records_in

    except:
        return None


def write_data(outfile, records, fields):
    """Write the data in the state object to file and return boolean success indicator"""
    try:
        with open(outfile, "wb") as csvfile:
            if fields is not None:
                writer = csv.DictWriter(csvfile, fieldnames=fields, lineterminator="\n")
                writer.writeheader()
            else:
                writer = csv.writer(csvfile, lineterminator="\n")
            writer.writerows(records)
            return True
    except:
        return False


def get_tree(xmldata):
    """Takes string or ET and returns an ET"""
    if type(xmldata) == str:
        try:
            return ET.fromstring(xmldata)
        except:
            return None
    elif type(xmldata) == ET.Element:
        return xmldata
    else:
        return None


def oclc_search(searchtype, data, exact=True):
    """Query OCLC endpoint

    Valid searchtype values:
        isbn  (Either ISBN10 or ISBN13 identifier)
        issn  (ISSN-L preferred but p-ISSN or e-ISSN will work)
        title (Exact match search)
        bib   (Title and Author search)
        wi    (OCLC "work index" identifier)

    Data should be either a string object for ISBN/ISSN/WI/Title or
    a two-value string tuple of (<title>, <author>) as appropriate.

    Returns one of:
        A string of XML data on successful query
        Boolean False on invalid searchtype or data
        None object in event of error making request
    """

    # Basic sanity checks and query forming
    if searchtype in ['isbn', 'issn', 'wi', 'title']:
        if type(data) != str:
            return False
        if searchtype == "title" and exact:
            data = "\"" + data + "\""
        query = "%s=%s" % (searchtype, data)
    elif searchtype == "bib":
        if type(data) != tuple:
            return False
        if len(data) != 2:
            return False
        author, title = data
        if exact:
            query = "author=\"%s\"&title=\"%s\"" % (author, title)
        else:
            query = "author=%s&title=%s" % (author, title)
    else:
        # invalid searchtype
        return False

    request_url = endpoint_url + base_querystring + "&" + query

    try:
        response = requests.get(request_url)
        if response.status_code == 200:
            return response.content
        else:
            return None
    except:
        return None


def extract_response(record_xml):
    """Parse an OCLC Classify XML record, extract and return the response code

    Possible responses:
    0:    Success. Single-work summary response provided.
    2:    Success. Single-work detail response provided.
    4:    Success. Multi-work response provided.
    100:  No input. The method requires an input argument.
    101:  Invalid input. The standard number argument is invalid.
    102:  Not found. No data found for the input argument.
    200:  Unexpected error.
    (Source: http://classify.oclc.org/classify2/api_docs/classify.html)
    """
    tree = get_tree(record_xml)
    if tree is None:
        return None

    response_code = tree.find("classify:response", ns)
    if response_code is None:
        # Uh-oh!
        return None
    else:
        return int(response_code.attrib["code"])


def extract_ids(record_xml):
    """Parse an OCLC Classify XML record for a single work and extract DDC/LLC and the Work Identifier (wi)
    Takes a String or XML ETree object and returns a tuple of strings (<ddc id>, <llc id>) or None
    """
    tree = get_tree(record_xml)
    if tree is None:
        return None

    # Check OCLC response code is for a single work record
    # 0:    Success. Single-work summary response provided.
    # 2:    Success. Single-work detail response provided.
    code = extract_response(tree)
    if code not in [0, 2]:
        return None
    else:
        try:
            ddc = tree.find("classify:recommendations/classify:ddc/classify:mostPopular", ns).attrib["nsfa"]
        except:
            ddc = ""
        try:
            lcc = tree.find("classify:recommendations/classify:lcc/classify:mostPopular", ns).attrib["nsfa"]
        except:
            lcc = ""

        return ddc, lcc


def resolve_multiple(record_xml):
    """Parse an OCLC Classify XML record for a multiple-work response, extract and return the Work Identifier (wi)"""

    tree = get_tree(record_xml)
    if tree is None:
        return None

    # Check OCLC response code is for a multi record
    # 4:    Success. Multi-work response provided.
    code = extract_response(tree)
    if code != 4:
        return None
    else:
        wi = tree.find("classify:works/classify:work[0]", ns).attrib["wi"]
        return wi


def process_row(row, columns, skip_columns = None):
    """Process a row from the csv file. Main per-record logic"""

    # Does row already have data in any of the skip_columns?
    if skip_columns:
        skip = False
        for column in skip_columns:
            if row[column] != "":
                vprint("Data found in column %s, skipping row" % column)
                skip = True
        if skip:
            return row

    # Determine whether we are matching against ISBN/ISSN or bibliographic data
    # Start from least preferable and check each type, keeping current best in state variable
    search_type = None
    data = None
    if columns[3] and row[columns[3]] != "":  # title
        if columns[2] and row[columns[2]] != "":  # author
            search_type = "bib"
            data = (row[columns[3]], row[columns[2]])
        else:
            search_type = "title"
            data = row[columns[3]]
    if columns[1] and row[columns[1]] != "":  # issn
        search_type = "issn"
        data = row[columns[1]]
    if columns[0] and row[columns[0]] != "":  # isbn
        search_type = "isbn"
        data = row[columns[0]]

    if search_type is None:
        return row
    # Make the first query and check the status
    record = oclc_search(search_type, data, exact_searches)
    status = extract_response(record)

    if status is None or status >= 100:
        # Error or no input, return the unaltered input row
        return row
    elif status in [0, 2]:
        # Single work record, go to extraction
        if type(row) == dict:
            row["ddc"], row["lcc"] = extract_ids(record)
        elif type(row) == list:
            row.extend(extract_ids(record))
        return row

    elif status == 4:
        # Multi-work record, attempt to resolve
        wi = resolve_multiple(record)
        if wi:
            parent_record = oclc_search("wi", wi)
            parent_status = extract_response(parent_record)
            if parent_status in [0, 2]:
                # Resolved, extract the IDs
                if type(row) == dict:
                    row["ddc"], row["lcc"] = extract_ids(parent_record)
                elif type(row) == list:
                    row.extend(extract_ids(parent_record))
                return row
            else:
                return None


def find_field(field, columns):
    """Attempt to find a potential field from the CSVs columns"""
    columns_lower = [data.lower() for data in columns]

    if field.lower() in columns_lower:
        # Easy-peasy! Just make sure we return the column name from file in case of caps differences :D
        return columns[columns_lower.index(field.lower())]

    potentials = [column for column in columns if field.lower() in column.lower()]
    if len(potentials) >= 1:
        return potentials[0]

    return None


def vprint(text):
    """Print a line of text to screen only if -v flag was set"""
    if verbose:
        print(text)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="""A tool to retrieve DDC/LCC identifiers from OCLC's Classify API
    
Expects an input CSV of 4 columns: ISBN,ISSN,Author,Title
For other formats:
    -f will search for the best matches amongst the fields from the first line of the file
    -c allows you to provide column numbers for the data""")

    parser.add_argument("-v", "--verbose", action="store_true", help="Display extra messages (search details etc)")
    fields = parser.add_mutually_exclusive_group()
    fields.add_argument("-f", "--fields", action="store_true",
                        help="Read field names from first line of CSV file and attempt to automagically determine \
                             correct columns")
    fields.add_argument("-c", dest="columns", nargs=4, metavar=('0', '1', '2', '3'),
                        help="Supply 0-based column numbers for ISBN, ISSN, Author and Title. If particular data \
                             not present, use 'None'")
    parser.add_argument("-s", "--skip", action="store_true", help="Treat first line of input CSV as a header and skip")
    parser.add_argument("-n", "--non-exact", action="store_true", help="Allow non-exact matching of author and title")
    parser.add_argument("-e", "--except", dest="skip_columns", help="Supply a comma separated list of column names, \
                                                                rows with data in any of these columns will be skipped")
    parser.add_argument("infile", help="Input CSV file")
    parser.add_argument("outfile", help="Output CSV file")
    args = parser.parse_args()

    print("subjectify.py: A tool to retrieve DDC/LCC identifiers from OCLC's Classify API\n")

    if args.columns and args.skip_columns:
        sys.exit("-e option is currently incompatible with -c")

    if args.verbose:
        print("Enabling verbose mode")
        verbose = True

    if args.non_exact:
        print("Enabling non-exact matches")
        exact_searches = False

    print("Loading data from %s" % args.infile)

    if args.skip:
        print("Skipping header row")
        skip_header = True
    else:
        skip_header = False

    if args.fields:
        records_in = load_data(args.infile, fields="file", skipheader=False)  # -f flag implies there must be a header!
        output_fields = records_in[0].keys() + ["ddc", "lcc"]
        # Lets see if we can find the fields automatically
        file_fields = records_in[0].keys()
        potentials = {"ISBN": None,
                      "ISSN": None,
                      "Author": None,
                      "Title": None}
        for item in potentials:
            potentials[item] = find_field(item, file_fields)

        print("Best match columns:")
        for item, val in potentials.iteritems():
            print("%s: %s" % (item, val))
        answer = raw_input("\nUse these columns? (Y/N): ")
        if answer in ["y", "Y"]:
            columns = [potentials["ISBN"], potentials["ISSN"], potentials["Author"], potentials["Title"]]
        else:
            sys.exit()

    elif args.columns:
        records_in = load_data(args.infile, fields="none", skipheader=skip_header)
        output_fields = None
        # Type the inputs
        columns = []
        for column in args.columns:
            try:
                column = int(column)
            except TypeError:
                column = None
            finally:
                columns.append(column)
        # Check the highest input is not greater than the number of fields
        if len(records_in[0]) < max(columns):
            sys.exit("Input column (%s) is greater than the number of fields in the CSV file (%s)"
                     % (max(columns), len(records_in[0])))
        # Confirm with user that the selected columns are correct
        print("Selected columns would provide this data for the first entry:")
        print("ISBN: %s" % records_in[0][columns[0]])
        print("ISSN: %s" % records_in[0][columns[1]])
        print("Author: %s" % records_in[0][columns[2]])
        print("Title: %s" % records_in[0][columns[3]])
        answer = raw_input("\nIs this correct? (Y/N): ")
        if answer in ["y", "Y"]:
            pass
        else:
            sys.exit()
    else:
        # Maybe could add a confirmation here in line with the other modes?
        records_in = load_data(args.infile, fields="default", skipheader=skip_header)
        output_fields = default_fields + ["ddc", "lcc"]
        columns = default_fields

    if args.skip_columns:
        skip_columns = args.skip_columns.split(",")
        print("Checking existence of fields for row skipping: %s" % skip_columns)
        skip_columns_lower = [data.lower() for data in skip_columns]
        valid_skip_columns = [column for column in records_in[0].keys() if column.lower() in skip_columns_lower]
        print("Fields found for row skipping: %s" % valid_skip_columns)
    else:
        valid_skip_columns = None

    print("Loaded %s records" % len(records_in))

    records_out = []
    for index, row in enumerate(records_in):
        print("Processing record %s" % (index+1))
        row_out = process_row(row, columns, valid_skip_columns)
        records_out.append(row_out)

        # Rate limiter, sleep 5s every 10 records and 5m every 250
        if index+1 % 10 == 0:
            if index+1 % 250 == 0:
                print("Rate limiter - sleeping 5 minutes")
                time.sleep(355)
            else:
                print("Rate limiter - sleeping 5 seconds")
                time.sleep(5)


    print("Finished processing, writing to file %s" % args.outfile)
    write_data(args.outfile, records_out, output_fields)

    print("Done, goodbye!")
