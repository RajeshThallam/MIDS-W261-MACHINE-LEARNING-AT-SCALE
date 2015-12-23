#!/usr/bin/python
import sys
import ast
from pyspark import SparkContext

def pagerank_init(line):
    # outgoing links with node
    node, ol = line.split('\t')
    for link in ast.literal_eval(ol).keys():
        yield node.encode('utf-8') + ' ' + link
            
if __name__ == "__main__":
    # Initialize the spark context.
    sc = SparkContext(appName="ConvertWiki")
    lines = sc.textFile(sys.argv[1], 1).flatMap(lambda pages: pagerank_init(pages)).saveAsTextFile(sys.argv[2])
    sc.stop()