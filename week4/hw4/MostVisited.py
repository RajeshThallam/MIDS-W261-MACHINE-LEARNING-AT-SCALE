#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.conf import combine_dicts

class MRMostVisited(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_pages,
                combiner=self.combiner_count_pages,
                reducer=self.reducer_count_pages),
            MRStep(reducer=self.reducer_find_most_visited)
        ]

    def mapper_get_pages(self, _, line):
        page = line.split(',')[1].strip()
        yield (page, 1)

    def combiner_count_pages(self, page, counts):
        yield (page, sum(counts))

    def reducer_count_pages(self, page, counts):
        yield None, (sum(counts), page)
        
    def reducer_find_most_visited(self, _, page_count_pairs):
        # each item of page_count_pairs is (count, word),
        # so yielding one results in key=counts, value=page
        results = sorted(list(page_count_pairs), key = lambda x: x[0], reverse = True)[:5]
        for p in results:
            yield int(p[1]),p[0]

if __name__ == '__main__':
    MRMostVisited.run()