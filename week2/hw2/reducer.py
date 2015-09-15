#!/usr/bin/python
import traceback
import sys

try:
    word_counts = {}

    # read each map output
    for line in sys.stdin:
        # parse mapper output
        word, count = line.strip('\n').split('\t')
        
        try:
            word_counts[word] += int(count)
        except:
            word_counts[word] = int(count)
        
    print word_counts
except Exception: 
    traceback.print_exc()