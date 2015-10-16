#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.conf import combine_dicts

class MRLongestNgram(MRJob):
    def mapper(self, _, line):
        [ngram, count, page, book] = line.lower().strip().split("\t")
        yield None, (len(ngram), ngram)
    
    def combiner(self, _, line):
        yield None, max(line)
    
    def reducer(self, _, ngram_length):
        yield max(ngram_length)

if __name__ == '__main__':
    MRLongestNgram.run()