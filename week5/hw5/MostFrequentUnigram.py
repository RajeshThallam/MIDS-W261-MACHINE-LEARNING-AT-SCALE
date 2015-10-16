#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.conf import combine_dicts

class MRMostFrequentUnigram(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_longest_ngram)
        ]

    def mapper(self, _, line):
        ngram = line.split('\t')[0].strip()
        yield ngram, len(ngram)
        
    def reducer(self, ngram, length):
        yield None, (sum(length), ngram)
        
    def reducer_longest_ngram(self, _, ngram_length_pair):
        yield max(ngram_length_pair)

if __name__ == '__main__':
    MRMostFrequentUnigram.run()