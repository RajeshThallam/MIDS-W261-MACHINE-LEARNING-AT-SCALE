from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MRNodeEdgeCounts(MRJob):

    def mapper(self, _, line):
        v, edges = line.strip().split('\t')
        edges = eval(edges)
        degree = len(edges.items())
        yield None, (1, degree)
        
    def reducer(self, _ ,count):
        data = list(count)
        nodes, edges = sum([i[0] for i in data]), sum([i[1] for i in data])
        yield nodes, edges

if __name__ == '__main__':
    MRNodeEdgeCounts.run()