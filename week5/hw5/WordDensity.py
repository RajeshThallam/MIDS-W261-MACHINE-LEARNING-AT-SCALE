#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys

class MRWordDensity(MRJob):

    def mapper_init(self):
        self.word_density = {}

    def mapper(self, _, line):
        [ngram, count, page, book] = line.lower().strip().split("\t")
        for word in ngram.split(' '):
            if word in self.word_density:
                self.word_density[word][0] += int(count)
                self.word_density[word][1] += int(page)
            else:
                self.word_density[word] = []
                self.word_density[word].append(int(count))
                self.word_density[word].append(int(page))

    def mapper_final(self):
        for word in self.word_density:
            yield word, self.word_density[word]

    def combiner(self, word, counts):
        counts = [c for c in counts]
        count = sum([ c[0] for c in counts])
        page  = sum([ c[1] for c in counts])
        yield word, (count, page)

    def reducer(self, word, counts):
        counts = [c for c in counts]
        count = sum([ c[0] for c in counts])
        page  = sum([ c[1] for c in counts])
        density = int(count) * 1.0 /int(page)
        yield word, density

if __name__ == '__main__':
    MRWordDensity.run()