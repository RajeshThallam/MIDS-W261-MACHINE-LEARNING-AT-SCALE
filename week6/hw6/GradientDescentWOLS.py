#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep

# Mapper: calculate partial gradient for each example
class MRJobGradientDescentWOLS(MRJob):

    # run before the mapper processes any input
    def read_weightsfile(self):
        self.weights = [ float(x) for line in open('weights.txt').read().strip().split('\n') for x in line.split(',') ]
        # Read weights file
        with open('weights.txt', 'r')  as f:
            for line in f:
                val = line.strip().split(',')
        self.weights = [float(x) for x in val]
        self.partial_Gradient = [0] * len(self.weights)
        self.partial_count = 0

    # calculate partial gradient for each example 
    def partial_gradient(self, _, line):
        y_i, x_i = (map(float,line.split(',')))

        w_i = 1 / abs(x_i)
        intercept, slope = self.weights
        y_hat = slope * x_i + intercept

        gd_intercept = w_i * (y_hat - y_i)
        gd_slope = w_i * (y_hat - y_i) * x_i

        self.partial_Gradient[0] += gd_intercept
        self.partial_Gradient[1] += gd_slope
        self.partial_count += 1
    
    # Finally emit in-memory partial gradient and partial count
    def partial_gradient_emit(self):
        yield None, (self.partial_Gradient, self.partial_count)
        
    # Accumulate partial gradient from mapper and emit total gradient 
    # Output: key = None, Value = gradient vector
    def gradient_accumulater(self, _, partial_Gradient_Record): 
        total_gradient = [0]*2
        total_count = 0
        for partial_Gradient,partial_count in partial_Gradient_Record:
            total_count = total_count + partial_count
            total_gradient[0] = total_gradient[0] + partial_Gradient[0]
            total_gradient[1] = total_gradient[1] + partial_Gradient[1]
        yield None, [v/total_count for v in total_gradient]

    def steps(self):
        return [MRStep(mapper_init=self.read_weightsfile,
                        mapper=self.partial_gradient,
                        mapper_final=self.partial_gradient_emit,
                        reducer=self.gradient_accumulater)] 
    
if __name__ == '__main__':
    MRJobGradientDescentWOLS.run()