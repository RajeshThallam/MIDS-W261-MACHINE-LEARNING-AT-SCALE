#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import math

class MRSimilarityCalculator(MRJob):    
    def mapper(self, _, line):
        # read streaming word occurrence input and evaluate stripes
        stripes = line.strip().split('\t')

        word = stripes[0].strip('"')
        stripe_pairs = ast.literal_eval(stripes[1])

        # read second instance of word cooccurrences broadcasted to the mappers
        for line in open('sample_data.txt', 'r').read().strip().split('\n'):
            # parse word and stripes
            l_stripes = line.split('\t')

            l_word = l_stripes[0].strip('"')
            l_stripe_pairs = ast.literal_eval(l_stripes[1])

            # avoid symmetric calculations
            # (a, b) = (b, a)
            if word >= l_word:
                continue

            # form combined keys to check distance each word 
            combined_words = set(l_stripe_pairs.keys()).union(set(stripe_pairs.keys()))

            # euclidean distance
            euclidean_distance = 0.0
            euclidean_distance = math.sqrt(sum((stripe_pairs.get(k, 0) - l_stripe_pairs.get(k, 0))**2 for k in combined_words))

            # cosine similarity
            cosine_similarity = 0.0
            # norm X
            xx = sum(stripe_pairs.get(k, 0)**2 for k in combined_words)
            # norm Y
            yy = sum(l_stripe_pairs.get(k, 0)**2 for k in combined_words)
            # norm XY
            xy = sum(stripe_pairs.get(k, 0)*l_stripe_pairs.get(k, 0) for k in combined_words)            
            cosine_similarity = xy/math.sqrt(xx*yy)

            # emit word and similarity metrics
            yield ((word, l_word), (euclidean_distance, cosine_similarity))

if __name__ == '__main__':
    MRSimilarityCalculator.run()
