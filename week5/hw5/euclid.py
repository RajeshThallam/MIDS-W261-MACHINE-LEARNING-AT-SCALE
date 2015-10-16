#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import re
import ast
from itertools import combinations

class Euclidean(MRJob):
    
    def steps(self):
        return [MRStep(mapper = self.mapper,
                       reducer = self.reducer),
                MRStep(reducer = self.euclidean)]

    def mapper(self, _, line):

        line = line.strip()
        key, stripe = re.split('\t',line)
        if key != 'null':
            key = re.sub('\"','',key)
            stripe = ast.literal_eval(stripe)
            s = sum(stripe)
            normalized_count = [float(x)/s for x in stripe]
            for i in xrange(len(stripe)):
                yield i, (key, stripe[i])
    
    def reducer(self, colnum, values):
        # all possible combination
        combos = combinations(values, 2)
        for combo in combos:
            diff = combo[0][1] - combo[1][1]
            key = tuple(sorted((combo[0][0],combo[1][0])))
            yield key, diff**2
    
    def euclidean(self, key, sqr_diff):
        sum_sqrt = sum(sqr_diff)
        eucld = math.sqrt(sum_sqrt)
        yield key, eucld

if __name__ == '__main__':
    Euclidean.run()
