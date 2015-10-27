#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys

class MRReadShortestPath(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, line):
        # parse input
        line = line.split('\t')

        node = line[0].strip('"')
        stripe = line[1].strip('"').split('|')
        edges = stripe[0]
        dist = stripe[1]
        path = stripe[2]
        state = stripe[3].replace('"', "")
        
        if state[0] == 'F':
            path = ast.literal_eval(path)
            words = [ lookup[w] for w in path ]
            end_word = lookup[node]
            shortest_path = ' -> '.join(words) + " -> " + end_word
            min_distance = dist
            
            yield shortest_path, min_distance
        else:
            pass
        
if __name__ == '__main__':
    MRReadShortestPath.run()