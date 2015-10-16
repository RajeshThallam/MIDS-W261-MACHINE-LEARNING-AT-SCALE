#!/usr/bin/env python

from mrjob.job import MRJob

F_LOOKUP = 'page_urls.txt'

class InnerJoin(MRJob):
            
    def mapper_init(self):
        self.urls = { k:v.strip(' "') for k, v in (line.split(",") for line in open(F_LOOKUP).read().strip().split('\n')) }
                
    def mapper(self, _, line):
        tokens = line.strip().split(',')
        
        if tokens[1] in self.urls:
            yield tokens[1], (tokens[-1], self.urls.get(tokens[1], ""))

if __name__ == '__main__':
    InnerJoin.run()