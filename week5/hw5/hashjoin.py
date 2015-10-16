#!/usr/bin/env python

from hashinnerjoin import InnerJoin
from hashleftjoin import LeftJoin
from hashrightjoin import RightJoin
import sys

SOURCE = './transformed_msweb_log.out'

join_type = sys.argv[1]
if join_type not in ['inner', 'left', 'right']:
    print "Invalid join type. Type should be inner, left or right."
    sys.exit(1)

if join_type == "inner":
    mr_job = InnerJoin(args=[SOURCE, '--file', 'page_urls.txt'])
elif join_type == "left":
    mr_job = LeftJoin(args=[SOURCE, '--file', 'page_urls.txt'])
elif join_type == "right":
    mr_job = RightJoin(args=[SOURCE, '--file', 'page_urls.txt'])
else:
    print "Invalid join type. Type should be inner, left or right."
    sys.exit(1)

with mr_job.make_runner() as runner: 
    runner.run()
    count = 0
    
    print "-" * 60
    print "{0:<25} {1:<5} {2:<6}".format("url", "page", "visit" )
    print "-" * 60
    for line in runner.stream_output():
        if count <= 10:
            page, value =  mr_job.parse_output_line(line)
            visitor, url = value
            print "{0:<25} {1:<5} {2:<6}".format(url, page, visitor)
        count = count + 1
    print "-" * 60
    print "Row count - {} joining table left with table right = {}".format(join_type, count)
    print "-" * 60