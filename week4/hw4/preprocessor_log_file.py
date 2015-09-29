#!/usr/bin/env python
import sys
import re

file_name = sys.argv[1]
valid_records = re.compile('^C|^V|^A')

f_urls = open('page_urls.txt','w')
f_log  = open('transformed_msweb_log.out','w')

# read file treating new line as row separator 
for line in open(file_name).read().strip().split('\n'):
    # read only if rows start with C or V
    if valid_records.search(line):
        terms = line.split(",")
        # if row starts with C store the visitor id
        if terms[0] == 'C':
            case = terms[1].strip('"')
        # if row starts with V, print the visitor id with page
        # in the format defined
        if terms[0] == 'V':
            print >>f_log, "{},{},{},{},{}".format(terms[0], terms[1], terms[2], "C", case)
        if terms[0] == 'A':
            print >>f_urls, "{},{}".format(terms[1], terms[4])