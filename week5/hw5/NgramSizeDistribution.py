#!/usr/bin/env python
from mrjob.job import MRJob

class MRNgramSizeDistribution(MRJob):
    def mapper(self, _, line):
        ngram = line.split('\t')[0].strip()
        yield len(ngram), 1

    def combiner(self, size, count):
        yield int(size), sum(count)
        
    def reducer(self, size, count):
        yield int(size), sum(count)
        
if __name__ == '__main__':
    MRNgramSizeDistribution.run()