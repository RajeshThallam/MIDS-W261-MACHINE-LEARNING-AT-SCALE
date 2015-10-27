#!/usr/bin/env python

from NodeEdgeCounts import MRNodeEdgeCounts
from DegreeDistribution import MRDegreeDistribution
import time
import sys

SOURCE = sys.argv[1]
RUNMODE = sys.argv[2]

start_time = time.time()
print "processing file {}".format(SOURCE)

mr_job = MRNodeEdgeCounts(args=[SOURCE, '-r', RUNMODE, '--pool-emr-job-flows', '--max-hours-idle', '1'])
with mr_job.make_runner() as runner: 
    runner.run()
    nodes = []
    edges = []
    for line in runner.stream_output():
        nodes.append(int(line.split('\t')[0]))
        edges.append(int(line.split('\t')[1]))
        
    node_count = sum(nodes)
    edge_count = sum(edges)

mr_job = MRDegreeDistribution(args=[SOURCE, '-r', RUNMODE, '--pool-emr-job-flows', '--max-hours-idle', '1'])

f = open('./degree_distribution.txt', 'w+')
degree = {}

with mr_job.make_runner() as runner: 
    runner.run()
    for line in runner.stream_output():
        f.writelines(line)
        line = line.split('\t')
        degree[line[0]] = int(line[1])
        
f.close()
end_time = time.time()
print "Time taken to do analysis = {:.2f} seconds".format(end_time - start_time)

print "Number of nodes = {}".format(node_count)
print "Number of links = {}".format(edge_count)
print "Average Degree = {}".format(1.0 * sum(degree.values())/len(degree))