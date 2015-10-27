#!/usr/bin/python
from dijkstra import MRDijkstra
import time
import sys
import ast

# read source file, start and end nodes
SOURCE = sys.argv[1]
START_NODE = sys.argv[2]

if len(sys.argv) == 4:
    END_NODE = sys.argv[3]

start_time = time.time()
print "processing file {}".format(SOURCE)

# run on local
mr_job = MRDijkstra(args=[SOURCE, '--startnode', START_NODE, '--endnode', END_NODE, '-q', '-r', 'local', '--no-strict-protocol'])

counter = 0
prev_counters = [{}]

# run iterations until all nodes are visited or target is reached
while (1):
    with mr_job.make_runner() as runner: 
        counter += 1
        print "iteration #{}".format(counter)

        # run the mapreduce job 
        # each iteration of MapReduce job of the algorithm expands 
        # the "search frontier" by one hop
        runner.run()

        # stream the output of mapreduce to local
        f = open(SOURCE, 'w+')

        # read the output and write to local file
        for line in runner.stream_output():
            f.writelines(line)
            line = line.split('\t')

            node = line[0].strip('"')
            stripe = line[1].strip('"').split('|')
            edges = stripe[0]
            dist = stripe[1]
            path = stripe[2]
            state = stripe[3].replace('"', "")

            # if target node is reached expand the path
            if state[0] == 'F' :
                path = ast.literal_eval(path)
                shortest_path = '->'.join(path) + "->" + node
                min_distance = dist
            
        # termination condition
        # only for local mode
        # read the counters and find all nodes are visited or target reached
        # break the iterations
        curr_counters = runner.counters()
        visited = curr_counters[0]['group'].get('visited', -1)
        print curr_counters
        if curr_counters == [{}] or curr_counters == prev_counters or visited == 0:
            break
        prev_counters = runner.counters()

    f.close()

end_time = time.time()
print "Time taken to find shortest path = {:.2f} seconds".format(end_time - start_time)
print "shortest path in the network from node {} to node {} is {} with distance of {}".format(START_NODE, END_NODE, shortest_path, min_distance)