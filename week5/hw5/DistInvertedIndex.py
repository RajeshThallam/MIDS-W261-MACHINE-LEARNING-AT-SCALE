#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, sys
import ast
from mrjob.protocol import RawValueProtocol,JSONProtocol
from itertools import combinations

class DistanceCalcInvertedIndex(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer
                  ),
            MRStep(mapper=self.mapper_dist,
                   reducer=self.reducer_dist
                  )
        ]

    # Step:1
    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        
        docId = tokens[0].replace("\"","")
        stripe = ast.literal_eval(tokens[1])
        
        for word,cnt in stripe.iteritems():
            yield word, (docId, int(cnt))
    
    def reducer(self, word, docId_count):
        docId_counts = [i for i in docId_count]
        yield word, docId_counts
        
    # Step: 2
        
    def mapper_dist(self, key, docId_counts):
        #sys.stderr.write('{0} # {1}\n'.format(key, docId_counts))
        docId_counts = sorted(docId_counts)
        l = len(docId_counts)
        for i in xrange(l):
            for j in xrange(l):
                if j > i:
                    yield (docId_counts[i][0], docId_counts[j][0]), (docId_counts[i][1] * docId_counts[j][1])
        
        
    def reducer_dist(self, docId_pair, values):
        yield docId_pair, sum(values)
        
if __name__ == '__main__':
    DistanceCalcInvertedIndex.run()
