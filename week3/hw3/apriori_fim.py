#!/usr/bin/python
import traceback
import fim

try:
    item_counts = {}
    frequencies = []
    rules = []

    # read input file by line and split to 
    # store each line as list of items
    # fim apriori expects this data structure as input
    baskets = [ line.split() for line in open('ProductPurchaseData.txt').read().strip().split('\n')]
    
    # target = 's'       -> frequent item sets
    # supp   = negative  -> minimum support of an item set
    # zmax   = number    -> maximum number of items per item set
    item_sets = fim.apriori(baskets, target='s', supp=-100, zmax=2)
    
    for r in item_sets:
        # apriori reports in the format ((itemset), support)
        item_set, item_count = r
        # k = 1
        if len(item_set) == 1:
            item_counts[item_set[0]] = item_count
        # k = 2
        elif len(item_set) == 2:
            item1, item2 = item_set
            # lexicographial ordering of the rules
            # report the rule a->b but not the rule b->a 
            if item1 < item2:
                frequencies.append(((item1, item2), float(item_count)))

    # calculate confidence
    for rule, count in frequencies:
        conf = count / item_counts[rule[0]]
        rules.append((rule[0], rule[1], conf))

    rules = sorted(rules, key=lambda x: (x[0], x[1]))
    rules = sorted(rules, key=lambda x: (x[2]), reverse=True)

    for rule in rules[:5]:
        print ("{} => {}, conf = {:.4f}").format(rule[0], rule[1], rule[2])

except Exception: 
    traceback.print_exc()