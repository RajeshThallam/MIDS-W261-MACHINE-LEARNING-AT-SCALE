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

    ol = links.split('|')
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
def combine_mass(prev, curr):
        # if the node is dangling, redistribute the loss
        # to all the nodes in the graph
        if type(prev) == float:
            if type(curr) == float:
                return [prev + curr]
            else:
                return [prev, curr]
        elif type(prev) == list:
            r_new = 0.0
            out = []
            
            for i in prev:
                if type(i) == float:
                    if type(curr) == float:
                        r_new = i + curr 
                    else:
                        out.append(curr)
                        out.append(i)
                else:
                    out.append(i)

            if r_new <> 0.0:
                out.append(r_new)
            return out
        else:
            return [prev, curr]    

def update_pagerank(x, loss, N, a = 0.15):
    r = 0.0
    out_links = ""

    node = x[0]
    
    if type(x[1]) == float:
        r = float(x[1])
    elif type(x[1]) == list:
        if type(x[1][0]) == float:
            r = x[1][0]
            out_links = x[1][1] 
        else:
            out_links = x[1][0] 
            r = x[1][1]
    else:
        out_links = x[1]

    r_new = a * (1/N) + (1-a) * (loss/N + r)
    return node, [round(r_new, 5), out_links]
            
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <source_file> <iterations> <target_file>")
        exit(-1)

    # Initialize the spark context.
    sc.appName = "PythonPageRank"

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
                    .reduceByKey(lambda prev, curr: combine_mass(prev, curr)).cache()
        
        # find dangling mass
        dangling_nodes = contribs.lookup('DANGLING')
        dangling_mass = 0.0 if len(dangling_nodes) == 0 else dangling_nodes[0]

        # update page rank
        ranks_new = contribs \
                    .filter(lambda (k, v): k != 'DANGLING') \
                    .map(lambda x: update_pagerank(x, dangling_mass, N, a))
        ranks = ranks_new
                
    ranks.saveAsTextFile(sys.argv[4])