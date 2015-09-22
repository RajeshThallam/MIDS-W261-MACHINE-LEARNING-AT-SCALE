#!/usr/bin/python
import traceback
import sys
import ast

try:    
    # counts sets with Apriori algorithm.
    SUPPORT_THRESHOLD = 100
    rules = []
    final_item_sets = {}
    final_item_count = {}

    # read each map output
    for line in sys.stdin:
        item, item_sets, item_count = line.strip().split('\t')
        item_sets = ast.literal_eval(item_sets)
        item_count = ast.literal_eval(item_count)

        if final_item_sets.get(item, None):
            final_item_sets[item] = {}

        count = final_item_sets.get(item)
        for k, v in item_sets.iteritems():
            count[k] = count.get(k, 0) + v

        final_item_sets[item] = count
        final_item_count[item] = final_item_count.get(item, 0) + item_count
        
    for item, item_count in final_item_count.iteritems():
        if item_count >= SUPPORT_THRESHOLD:
            for k, v in final_item_sets[item].iteritems():
                if v >= SUPPORT_THRESHOLD:
                    conf = float(v) / float(item_count)
                    rules.append((item, k, conf))

    rules = sorted(rules, key=lambda x: (x[0], x[1]))
    rules = sorted(rules, key=lambda x: (x[2]), reverse=True)
    
    for rule in rules[:5]:
        print ("{} -> {}, conf = {:.2f}").format(rule[0], rule[1], rule[2])
    
except Exception: 
    traceback.print_exc()