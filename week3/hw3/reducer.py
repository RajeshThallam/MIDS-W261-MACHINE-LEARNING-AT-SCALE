#!/usr/bin/python
import traceback
import itertools
import sys
import ast
import re

try:    
    # define variables
    SUPPORT_THRESHOLD = 100
    LEXIC_ORDERING = 1
    rules = []
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
            
    supps.append({k:v for k,v in final_item_set_0.iteritems() if v >= SUPPORT_THRESHOLD})
    print "|C{}| = {}".format(1, str(len(final_item_set_0)))
    print "|L{}| = {}".format(1, str(len(supps[0])))
    
    supps.append({k:v for k,v in final_item_set_1.iteritems() if v >= SUPPORT_THRESHOLD})
    print "|C{}| = {}".format(2, str(len(final_item_set_1)))
    print "|L{}| = {}".format(2, str(len(supps[1])))

    if LEXIC_ORDERING == 0:
        remove_chars = ['(', ')', ',', '\'']
        rx = '[' + re.escape(''.join(remove_chars)) + ']'

        for key, value in supps[1].iteritems():
            key = key.split(',')
            for a in itertools.combinations(key, 1):
                b = tuple([w for w in key if w not in a])
                conf = float(value) / float(supps[0][','.join(a)])

                a = re.sub(rx, '', str(a))
                b = re.sub(rx, '', str(b))
                rules.append((a, b, conf))
    else:
        for key, value in supps[1].iteritems():
            key = key.split(',')
            a = key[0]
            b = key[1]
            conf = float(value) / float(supps[0][a])
            rules.append((a, b, conf))
    
    rules = sorted(rules, key=lambda x: (x[0], x[1]))
    rules = sorted(rules, key=lambda x: (x[2]), reverse=True)

    print "-" * 60
    print 'Top-5 association rules with confidence scores'
    print "-" * 60

    for rule in rules[:5]:
        print ("{} => {}, conf = {:.4f}").format(rule[0], rule[1], rule[2])
    
except Exception: 
    traceback.print_exc()