#!/usr/bin/python
import traceback
import itertools
import sys
import re

try:
    item_sets = {}
    item_counts = {}

    for transaction in sys.stdin:
        items = sorted(list(set(transaction.split())))

        for item in items:
            item_counts[item] = item_counts.get(item, 0) + 1
        
        # using pairs and k = 2
        for pair in itertools.combinations(items, 2):
            key = ','.join(pair)
            item_sets[key] = item_sets.get(key, 0) + 1

    for k, v in item_sets.iteritems():
        print "{0}\t{1}".format(k, v)
        
    for k, v in item_counts.iteritems():
        print "{0}\t{1}".format(k, v)
        
except Exception: 
    traceback.print_exc()