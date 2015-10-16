#!/usr/bin/env python
from MostFrequentUnigrams import MRMostFrequentUnigrams
import time

start_time = time.time()

# local testing
#!./MostFrequentUnigrams.py -r local /home/rt/wrk/w261/hw5/data/googlebooks-eng-all-5gram-20090715-95-filtered.txt --no-strict-protocol 2>/dev/null > frequent_unigrams_sample.out
#!cat frequent_unigrams_sample.out | sort -k1nr | head -10000 > frequent_unigrams_sample.txt
#!head -10 frequent_unigrams_sample.txt

# on EMR
!MostFrequentUnigrams.py \
    -r emr s3://filtered-5grams \
    --output-dir=s3://ucb-mids-mls-rajeshthallam/hw_5_3/most_frequent_unigrams \
    --no-output \
    --no-strict-protocol
    
end_time = time.time()
print "Time taken to find most frequent unigrams = {:.2f} seconds".format(end_time - start_time)