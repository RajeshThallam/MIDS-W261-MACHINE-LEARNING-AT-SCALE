#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRMostFrequentUnigrams(MRJob):

    def mapper_init(self):
        self.support_counts = {}

    def mapper(self, _, line):
        [ngram, count, page, book] = line.lower().strip().split("\t")
        for word in ngram.split(' '):
            self.support_counts[word] = self.support_counts.get(word, 0) + int(count)

    def mapper_final(self):
        for word in self.support_counts:
            yield word, self.support_counts[word]

    def combiner(self, word, count):
        yield word, sum(count)

    def reducer(self, word, count):
        yield word, sum(count)

if __name__ == '__main__':
    MRMostFrequentUnigrams.run()