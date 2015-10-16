#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools
import sys

class MRFrequentBigrams(MRJob):

    '''
    # define MRJob steps
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                mapper_final=self.mapper_final,
                #combiner=self.combiner,
                reducer_init=self.reducer_init,
                reducer=self.reducer)
        ]
    '''

    # load top 10000 frequently appearing words into each memory of each mapper
    def mapper_init(self):
        #self.top_unigrams = { k.strip(' "'):v for k, v in (line.split("\t") for line in open('frequent_unigrams_10K.txt').read().strip().split('\n')) }
        self.cooccurrences = {}
        self.top_unigrams = {}
        
        for line in open('frequent_unigrams_10K.txt').read().strip().split('\n'):
            ngram = line.split('\t')
            self.top_unigrams[ngram[0].strip(' "')] = ngram[1]
    
    # emit cooccuring words with count = 1
    def mapper(self, _, line):
        line = line.strip().lower().split('\t')
        # select only words from the 5-gram that exists in the top 10000
        words = [ word for word in line[0].split() if word in self.top_unigrams.keys() ]
        
        # find bigram co-occurrences
        for word1, word2 in itertools.combinations(words, 2):
            if word1 in self.cooccurrences.keys():
                self.cooccurrences[word1][word2] = self.cooccurrences[word1].get(word2, 0) + int(line[1])
            else:
                self.cooccurrences[word1] = {word2: int(line[1])}

    # emit cooccuring words with count = 1
    def mapper_final(self):
        for k, v in self.cooccurrences.iteritems():
            yield (k, v)

    # combine word cooccurrences from the same mapper and emit stripes
    '''
    def combiner(self, word, cooccurrences):
        stripes = {}

        for stripe in cooccurrences:
            for k, v in stripe.iteritems():
                stripes[k] = stripes.get(k, 0) + v

        yield (word, stripes)

    def reducer_init(self):
        #self.top_unigrams = { k.strip(' "'):v for k, v in (line.split("\t") for line in open('frequent_unigrams_10K.txt').read().strip().split('\n')) }
        self.top_unigrams = {}
        
        for line in open('frequent_unigrams_10K.txt').read().strip().split('\n'):
            ngram = line.split('\t')
            self.top_unigrams[ngram[0].strip(' "')] = ngram[1]
    '''

    # emit word cooccurrences as stripes
    def reducer(self, word, cooccurrences):
        stripes = {}

        for stripe in cooccurrences:
            for k, v in stripe.iteritems():
                stripes[k] = stripes.get(k, 0) + v
        
        '''
        out_stripes = []
        for unigram in self.top_unigrams:
            out_stripes.append(stripes.get(unigram, 0))
        '''
            
        yield (word, stripes)

if __name__ == '__main__':
    MRFrequentBigrams.run()