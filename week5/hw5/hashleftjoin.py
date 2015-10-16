#!/usr/bin/env python

from mrjob.job import MRJob

F_LOOKUP = 'page_urls.txt'

class LeftJoin(MRJob):
            
    def mapper_init(self):
        self.urls = { k:v.strip(' "') for k, v in (line.split(",") for line in open(F_LOOKUP).read().strip().split('\n')) }
        self.visitors = self.urls.keys()
                
    def mapper(self, _, line):
        tokens = line.strip().split(',')
        
        if tokens[1] in self.visitors:        
            self.visitors.remove(tokens[1])

        if tokens[1] in self.urls:
            yield tokens[1], (tokens[-1], self.urls.get(tokens[1], ""))

    def mapper_final(self):
        for key in self.visitors:
            yield key, (None, self.urls.get(key, ""))
            
if __name__ == '__main__':
    LeftJoin.run()