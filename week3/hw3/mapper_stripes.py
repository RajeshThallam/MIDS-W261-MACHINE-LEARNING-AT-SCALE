#!/usr/bin/python
import traceback
import sys
import re

try:
    item_sets = {}
    item_counts = {}

    for transaction in sys.stdin:
        items = sorted(list(set(transaction.split())))
        
        for item in items:
            item_counts[item] = item_counts.get(item, 0) + 1

        # k = 2
        for pair in itertools.combinations(transaction.split(), 2):
            if not item_sets.get(pair[0], None):
                item_sets[pair[0]] = {}

            count = item_sets.get(pair[0])
            count[pair[1]] = count.get(pair[1], 0) + 1

            item_sets[pair[0]] = count

    for item in item_sets:
        print "{0}\t{1}\t{2}".format(item, item_sets[item], item_counts[item])
        
except Exception: 
    traceback.print_exc()