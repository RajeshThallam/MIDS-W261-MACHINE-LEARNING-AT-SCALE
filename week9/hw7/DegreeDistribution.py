from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MRDegreeDistribution(MRJob):

    def mapper(self, _, line):
        v, edges = line.strip().split('\t')
        edges = eval(edges)
        degree = len(edges.items())
        yield '%010d'%int(degree), 1
        
    def reducer(self, degree, count):
        yield int(degree), sum([i for i in count])

if __name__ == '__main__':
    MRDegreeDistribution.run()