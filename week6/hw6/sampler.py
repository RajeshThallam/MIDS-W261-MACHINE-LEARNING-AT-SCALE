#!/usr/bin/python
import os
import random
from mrjob.job import MRJob
from math import sqrt
import ast

class MRSample(MRJob):
    def mapper_init(self):
        self.samples = []
        self.count = 0
        self.sample_size = 10000

    def mapper(self, key, line):
        sample_line = line

        self.count += 1
        if len(self.samples) <= self.sample_size:
            self.samples.append(sample_line)
        else:
            expected_prob = (self.sample_size * 1.0) / self.count
            actual_prob = random.random()
            if actual_prob <= expected_prob:
                index = random.randint(0, self.sample_size)
                self.samples[index] = sample_line

    def mapper_final(self):
        out = [self.count, self.samples]
        yield 1, out

    def reducer(self, n, vars):
        samples_from_mappers = []
        counts_from_mappers = []

        total_counts_from_mappers = 0

        for x in vars:
            input = ast.literal_eval(x)
            total_counts_from_mappers += input[0]

            counts_from_mappers.append(input[0])
            samples_from_mappers.append(input[1])

        i = 0
        for sample_set in samples_from_mappers:
            weight = counts_from_mappers[i] * 1.0 / total_counts_from_mappers
            number_of_needed_samples = int(round(weight * self.sample_size))

            for j in range(number_of_needed_samples):
                yield 1, sample_set.pop()

            i += 1

if __name__ == '__main__':
    MRSample.run()