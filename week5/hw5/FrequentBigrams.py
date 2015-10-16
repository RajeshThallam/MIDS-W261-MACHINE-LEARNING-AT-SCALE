#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools
import sys

class MRFrequentBigrams(MRJob):

    # define MRJob steps
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer)
        ]

    # load top 10000 frequently appearing words into each memory of each mapper
    def mapper_init(self):
        self.top_unigrams = { k.strip(' "'):v for v, k in (line.split("\t") for line in open('frequent_unigrams_10K.txt').read().strip().split('\n')) }
    
    # emit cooccuring words with count = 1
    def mapper(self, _, line):
        # select only words from the 5-gram that exists in the top 10000
        words = [ word for word in line.lower().split('\t')[0].split() if word in self.top_unigrams.keys() ]
        
        # find bigram co-occurrences
        cooccurences = {}
        for word1, word2 in itertools.combinations(words, 2):
            if word1 in cooccurences.keys():
                cooccurences[word1][word2] = cooccurences[word1].get(word2, 0) + 1
            else:
                cooccurences[word1] = {word2: 1}

        for k, v in cooccurences.iteritems():
            yield (k, v)

    # combine word cooccurrences from the same mapper and emit stripes
    def combiner(self, word, cooccurences):
        stripes = {}

        for stripe in cooccurences:
            for k, v in stripe.iteritems():
                stripes[k] = stripes.get(k, 0) + v

        yield (word, stripes)

    # emit word cooccurrences as stripes
    def reducer(self, word, cooccurences):
        stripes = {}

        for stripe in cooccurences:
            for k, v in stripe.iteritems():
                stripes[k] = stripes.get(k, 0) + v

        yield (word, stripes)

if __name__ == '__main__':
    MRFrequentBigrams.run()