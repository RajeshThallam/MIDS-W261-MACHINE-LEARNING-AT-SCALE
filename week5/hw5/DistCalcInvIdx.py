#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys
import math

class MRDistCalcInvIdx(MRJob):
    # define MRJob steps
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_idx,
                reducer=self.reducer_idx),
            MRStep(
                mapper_init=self.mapper_distance_init,
                mapper=self.mapper_distance,
                reducer=self.reducer_distance)
        ]

    # stage1: indexing
    def mapper_idx(self, _, line):
        line = line.strip().split('\t')
        
        word1 = line[0].strip('"')
        stripe = ast.literal_eval(line[1])
        
        for word2, count in stripe.iteritems():
            yield word2, (word1, int(count))

    def reducer_idx(self, word2, word1_count):
        word1_counts = [w for w in word1_count]
        yield word2, word1_counts

    # stage2: pairwise similarity using euclidean and cosine
    def mapper_distance_init(self):
        self.columns = {}
        self.top_unigrams = {}
        
        for line in open('frequent_unigrams_10K.txt').read().strip().split('\n'):
            ngram = line.split('\t')
            self.top_unigrams[ngram[0].strip(' "')] = ngram[1]
        
    def mapper_distance(self, word2, word1_counts):
        postings = []
        words = { x[0].strip('"'):int(x[1]) for x in word1_counts }

        for key in self.top_unigrams:
            postings.append((key, words.get(key, 0)))
        
        postings = sorted(postings)
        l = len(postings)

        for i in xrange(l):
            for j in xrange(l):
                if j > i:
                    x = postings[i][1]
                    y = postings[j][1]
                    yield (postings[i][0], postings[j][0]), ((x-y)**2, x*x, y*y, x*y)
        
    def reducer_distance(self, words, distance_measures):
        d = distance_measures

        # euclidean
        euclidean = math.sqrt(sum([l[0] for l in d]))
        
        # cosine similarity
        xx = sum([l[1] for l in d])
        yy = sum([l[2] for l in d])
        xy = sum([l[4] for l in d])
        if xx == 0 or yy == 0:
            cosine = 0
        else:
            cosine = xy/math.sqrt(xx*yy)
        
        if cosine > 0.0:
            yield (words), (euclidean, cosine)                 

if __name__ == '__main__':
    MRDistCalcInvIdx.run()