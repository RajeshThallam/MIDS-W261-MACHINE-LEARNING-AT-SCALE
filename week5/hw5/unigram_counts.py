#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class UnigramCount(MRJob):

    def mapper_init(self):
        self.support = {}

    def mapper(self, _, line):
        line = line.lower().strip()
        [ngram,count,pages,books] = re.split("\t",line)
        words = ngram.split(' ')
        for i in range(len(words)):
            self.support[words[i]] = self.support.get(words[i], 0) + int(count)
    
    def mapper_final(self):
        for word in self.support:
            yield word, self.support[word]
    
    def combiner(self, word, count):
        yield word, sum(count)
    
    def reducer(self, word, count):
        total = sum(count)
        yield word, total
            
if __name__ == '__main__':
    UnigramCount.run()
