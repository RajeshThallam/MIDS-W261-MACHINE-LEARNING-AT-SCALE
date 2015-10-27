#!/usr/bin/python
from dijkstra import MRDijkstra
import time
import sys
import ast

start_time = time.time()

# load indices file into memory
lookup = { k:v for k, v in (line.split("\t") for line in open('./indices.txt').read().strip().split('\n')) }

SOURCE = sys.argv[1]
START_WORD = sys.argv[2]
START_NODE = lookup[START_WORD]

if len(sys.argv) == 4:
    END_WORD = sys.argv[3]
    END_NODE = lookup[END_WORD]
    
# reverse lookup i.e. give word based on index
lookup = { v:k for k, v in (line.split("\t") for line in open('./indices.txt').read().strip().split('\n')) }
    
print "processing file {}".format(SOURCE)
print "start word {} ({})".format(START_WORD, START_NODE)
print "end word {} ({})".format(END_WORD, END_NODE)
    
# on local
mr_job = MRDijkstra(args=[SOURCE, '--startnode', START_NODE, '--endnode', END_NODE, '-q', '-r', 'local', '--no-strict-protocol'])

counter = 0
prev_counters = [{}]

while (1):
    with mr_job.make_runner() as runner: 
        counter += 1
        print "iteration #{}".format(counter)

        runner.run()

        f = open(SOURCE, 'w+')

        for line in runner.stream_output():
            f.writelines(line)
            line = line.split('\t')

            node = line[0].strip('"')
            stripe = line[1].strip('"').split('|')
            edges = stripe[0]
            dist = stripe[1]
            path = stripe[2]
            state = stripe[3].replace('"', "")

            if state[0] == 'F' :
                path = ast.literal_eval(path)
                words = [ lookup[w] for w in path ]
                end_word = lookup[node]
                shortest_path = ' -> '.join(words) + " -> " + end_word
                min_distance = dist
            
        curr_counters = runner.counters()
        visited = curr_counters[0]['group'].get('visited', -1)
        print curr_counters
        if curr_counters == [{}] or curr_counters == prev_counters or visited == 0:
            break
        
        prev_counters = runner.counters()

    f.close()

end_time = time.time()
print "Time taken to find shortest path = {:.2f} seconds".format(end_time - start_time)
print "shortest path in the synonyms from word '{}' to word '{}' is [ {} ] with distance of {}".format(START_WORD, END_WORD, shortest_path, min_distance)