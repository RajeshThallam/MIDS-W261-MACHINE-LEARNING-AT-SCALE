#!/usr/bin/python
import traceback
import itertools
import sys
import ast

try:    
    # define variables
    final_item_set_0 = {}
    final_item_set_1 = {}
    supps = []

    # read each map output
    for line in sys.stdin:
        key, value = line.strip().split('\t')
        value = int(value)
        
        k = len(key.split(','))
        if k == 1:
            final_item_set_0[key] = final_item_set_0.get(key, 0) + value
        if k == 2:
            final_item_set_1[key] = final_item_set_1.get(key, 0) + value

    for k, v in final_item_set_0.iteritems():
        print "{0}\t{1}".format(k, v)
        
    for k, v in final_item_set_1.iteritems():
        print "{0}\t{1}".format(k, v)

except Exception: 
    traceback.print_exc()