#!/usr/bin/python
import re
import sys
import os
import sys
import ast
from operator import add

from pyspark import SparkContext

def pagerank_init(lines):
    # initialize page rank as 1/N for all nodes with 
    # outgoing links and emit with graph structure
    for line in lines:
        node, ol = line.split('\t')
        neighbors = '|'.join(ast.literal_eval(ol).keys())
        yield node.encode('utf-8'), [1/N, neighbors]

def distribute(lines):
    for line in lines:
        node, rank_links = ast.literal_eval(str(line))
        
        """Calculates URL contributions to the rank of other URLs."""
        r = rank_links[0]
        links = rank_links[1]

        ol = str(links).split('|')
        Ni = len(ol)

        # if the node is for dangling (i.e. no outgoing link),
        # emit the loss to redistribute to all the incoming
        # links to the dangling node
        if (Ni == 1 and ol[0] == '') or Ni == 0:
            #yield 'DANGLING', r
            dangling_mass.add(r)
        else:
            r_new = float(r)/float(Ni)
            for l in ol:
                yield l, r_new

        # recover graph structure
        if links <> '':
            yield node, links

# update pagerank by combining the mass
def combine_mass(rank_links):
    r = 0.0
    out = ''

    for i in rank_links.split('~'):
        try:
            i = ast.literal_eval(i)
            if type(i) == float:
                r += i
            else:
                out = i if i else out
        except:
            out = i if i else out
            pass

    return str(r) + '~' + str(out)

def update_pagerank(lines):
    for line in lines:
        node, rank_links = ast.literal_eval(str(line))
        r = 0.0
        out = ""

        for i in str(rank_links).split('~'):
            try:
                i = ast.literal_eval(i)
                if type(i) == float:
                    r = float(i)
                else:
                    out = i if i else out
            except:
                out = i if i else out
                pass

        r_new = a * (1/N) + (1-a) * (l/N + r)
        yield node, [r_new, out]
            
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <source_file> <iterations> <target_file>")
        exit(-1)

    # Initialize the spark context.
    sc = SparkContext(appName="WikiPageRank")

    lines = sc.textFile(sys.argv[1], 1)
    N = 15192277.0
    #N = 11.0
    D = 0.85
    a = 0.15
        
    # parse and initialize pagerank
    ranks = lines.mapPartitions(pagerank_init)
    
    for iteration in range(int(sys.argv[2])):
        dangling_mass = sc.accumulator(0.0)
        # contribution from each page
        ranks_new = ranks \
                    .mapPartitions(distribute) \
                    .reduceByKey(lambda prev, curr: combine_mass(str(prev) + '~' + str(curr))) \
                    .cache()

        c = ranks_new.take(1)        
        l = dangling_mass.value

        ranks = ranks_new.mapPartitions(update_pagerank).cache()
        sys.stderr.write('iteration = ' + str(iteration) + ' ;DANGLING_MASS = ' + str(dangling_mass.value) + '\n')
        
        if iteration in [1, 9, 49]:
            top_100 = ranks.top(100, key = lambda (node, rank_links): rank_links[0])
            sc.parallelize(top_100) \
                .map(lambda (node, rank_links): str(node) + '|' + str(rank_links[0])) \
                .saveAsTextFile(sys.argv[3] + "/" + str(iteration))
            for t in top_100:
                sys.stderr.write(str(t) + '\n')

    sc.stop()