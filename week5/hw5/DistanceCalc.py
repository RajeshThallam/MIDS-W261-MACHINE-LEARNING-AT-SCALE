#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import re
import sys
import ast
import urllib2
import math


class DistanceCalc(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper)
        ]
    
    def mapper_init(self):
        # Load the file into memory
        self.counter = 0
        self.stripes = {}

        # Load File
        with open('sample_data.txt','r') as f:
            for line in f:
                tokens = line.strip().split('\t')
                self.stripes[tokens[0].replace("\"","")] = ast.literal_eval(tokens[1])
                self.increment_counter('distance', 'num_stripes_loaded', amount=1)
        
        sys.stderr.write('### of stripes Loaded: {0}\n'.format(len(self.stripes)))
        
    def mapper(self, _, line):
        dist_type = get_jobconf_value('dist_type')
        tokens = line.strip().split('\t')
        
        key = tokens[0].replace("\"","")
        dict_pairs = ast.literal_eval(tokens[1])
        
        for n_key, n_dict_pairs in self.stripes.iteritems():

            if key > n_key:
                continue
            
            self.counter += 1   
            if self.counter % 1000 == 0:
                self.set_status('# of Distances Calculated: {0}'.format(self.counter))
                
            distance = None
            
            if dist_type == 'euclid':

                # Calculate Euclidean Distance
                squared_distance = 0
                for k in n_dict_pairs.keys():
                    squared_distance += (dict_pairs.get(k, 0) - n_dict_pairs.get(k, 0)) ** 2
                    
                distance = math.sqrt(squared_distance)
                
            if dist_type == 'cosine':
                
                # Calculate Cosine Distance
                # Get the intersection of keys from both stripes
                norm_x = 0
                norm_y = 0
                dot_x_y = 0
                for k in self.stripes.keys(): # Iterate through entire key range once
                    norm_x += dict_pairs.get(k,0) * dict_pairs.get(k,0)
                    norm_y += n_dict_pairs.get(k,0) * n_dict_pairs.get(k,0)
                    dot_x_y += dict_pairs.get(k,0) * n_dict_pairs.get(k,0)
                    
                distance = float(dot_x_y) / (math.sqrt(norm_x) * math.sqrt(norm_y))
          
            self.increment_counter('distance', 'num_{0}_distances'.format(dist_type), amount=1)
            yield (distance), (key, n_key)

if __name__ == '__main__':
    DistanceCalc.run()
