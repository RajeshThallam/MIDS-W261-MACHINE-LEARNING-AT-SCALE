#!/usr/bin/python
import re
import sys
import os
import sys
import ast
from operator import add

from pyspark import SparkContext

def pagerank_init(line):
    # initialize page rank as 1/N for all nodes with 
    # outgoing links and emit with graph structure
    node, ol = line.split('\t')
    neighbors = '|'.join(ast.literal_eval(ol).keys())
    yield node.encode('utf-8'), [1/N, neighbors]

def distribute(node, rank_links):
    """Calculates URL contributions to the rank of other URLs."""
    r = rank_links[0]
    links = rank_links[1]

    ol = str(links).split('|')
    Ni = len(ol)

    # if the node is for dangling (i.e. no outgoing link),
    # emit the loss to redistribute to all the incoming
    # links to the dangling node
    if (Ni == 1 and ol[0] == '') or Ni == 0:
        yield 'DANGLING', r
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

def update_pagerank(node, rank_links, loss, N, a = 0.15):
    r = 0.0
    out_links = ""    
        
    for i in str(rank_links).split('~'):
        try:
            i = ast.literal_eval(i)
            if type(i) == float:
                r = float(i)
            else:
                out_links = i if i else out_links
        except:
            out_links = i if i else out_links
            pass
    
    r_new = a * (1/N) + (1-a) * (loss/N + r)
    return node, [round(r_new, 5), out_links]
            
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <source_file> <iterations> <target_file>")
        exit(-1)

    # Initialize the spark context.
    sc = SparkContext(appName="PythonPageRank")

    lines = sc.textFile(sys.argv[1], 1)
    N = 11.0
    D = 0.85
    a = 0.15

    # parse and initialize pagerank
    ranks = lines.flatMap(lambda pages: pagerank_init(pages))
    
    for iteration in range(int(sys.argv[2])):
        # contribution from each page
        contribs = ranks \
                    .flatMap(lambda (node, rank_links): distribute(node, rank_links)) \
                    .reduceByKey(lambda prev, curr: combine_mass(str(prev) + '~' + str(curr))).cache()
        
        # find dangling mass
        dangling_nodes = contribs.lookup('DANGLING')
        dangling_mass = 0.0 if len(dangling_nodes) == 0 else float(str(dangling_nodes[0]).strip('~'))

        # update page rank
        ranks_new = contribs \
                    .filter(lambda (k, v): k != 'DANGLING') \
                    .map(lambda (node, rank_links): update_pagerank(node, rank_links, dangling_mass, N, a))
        ranks = ranks_new
                
    ranks \
        .map(lambda (node, rank_links): (node, round(rank_links[0], 3), rank_links[1])) \
        .saveAsTextFile(sys.argv[3])
    
    sc.stop()