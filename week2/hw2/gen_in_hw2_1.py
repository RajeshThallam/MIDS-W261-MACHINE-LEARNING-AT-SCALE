#!/usr/bin/python
import random

N = 10000
# used random.sample to avoid replacement of same numbers
r = random.sample(range(N), N)

for n in r:
    print "{0} {1}".format(n, "NA")