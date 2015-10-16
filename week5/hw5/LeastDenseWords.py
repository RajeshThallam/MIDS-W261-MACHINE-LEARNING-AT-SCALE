#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

class MRDenseWords(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer),
             MRStep(
                mapper=self.mapper_dense_words,
                reducer=self.mapper_dense_words,
                jobconf={
                    'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                    'mapred.text.key.comparator.options': '-k1,1rn',
                }
            )
        ]
    
    def mapper(self, _, line):
        tokens = line.strip().split("\t")
        unigrams = tokens[0].split()
        for unigram in unigrams:
            yield unigram, (int(tokens[1]), int(tokens[2]))
    
    def combiner(self, unigram, count_pair):
        yield unigram, (sum(count_pair[0]), sum(count_pair[1]))
    
    def reducer(self, unigram, count_pair):
        yield unigram, (sum(count_pair[0]), sum(count_pair[1]))
        
    def mapper_dense_words(self, unigram, count_pair):
        density = (int(count_pair[0]) * 1.0 / int(count_pair[1]))
        yield density, unigram
        
    def reducer_dense_words(self, density, unigrams):
        for unigram in unigrams:
            yield unigram, density

if __name__ == '__main__':
    MRDenseWords.run()