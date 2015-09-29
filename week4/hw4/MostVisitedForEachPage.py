#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostVisitedForEachPage(MRJob):

    # define MRJob steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_pairs,
                   combiner=self.combiner_count_pairs,
                   reducer=self.reducer_count_pairs),
             MRStep(mapper=self.mapper_find_most_visited,
                   reducer_init=self.reducer_frequent_visitor_init,
                   reducer=self.reducer_frequent_visitor)
        ]

    # mapper: get pair of page_id and visitor_id with 
    # count 1 => (page,visit), 1
    def mapper_get_pairs(self, _, line):
        terms = line.strip().split(",")
        key = "{0},{1}".format(terms[1], terms[4])
        yield key, 1

    # combiner: local aggregate for each (page, visit) 
    # and get counts => (page,visit), c
    def combiner_count_pairs(self, key, counts):
        yield key, sum(counts)

    # reducer: get counts for each (page, visit) => (page,visit), c
    def reducer_count_pairs(self, key, counts):
        yield key, sum(counts)
    
    # mapper: to find most frequent visitor for each 
    # page emit (page, visit_count), visit
    def mapper_find_most_visited(self, key, value):
        terms = key.strip().split(",")
        new_key = "{0},{1}".format(terms[0], value)
        yield new_key, terms[1]

    # reducer_init: load url file to fetch in reducer stage
    def reducer_frequent_visitor_init(self):
        self.urls = { k:v.strip(' "') for k, v in (line.split(",") for line in open('./page_urls.txt').read().strip().split('\n')) }
             
    # reducer: emit most frequent users for each page with 
    # web URL (page, url, visit count, user counts), (list of users)
    def reducer_frequent_visitor(self, key, values):
        terms = key.strip().split(",")
        page = terms[0]
        visits = int(terms[1])
        visitors = list(values)

        k = '{0:<5} {1:<25} {2:<6} {3:<10}'.format(page, self.urls.get(page, 'NA'), visits, len(visitors))
        v = '{0}'.format(",".join(visitors[:10]))
        yield k, v

if __name__ == '__main__':
    MRMostVisitedForEachPage.run()