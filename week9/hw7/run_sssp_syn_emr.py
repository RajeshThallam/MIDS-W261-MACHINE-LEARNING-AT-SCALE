#!/usr/bin/python
from dijkstra import MRDijkstra
from ReadShortestPath import MRReadShortestPath
from mrjob.emr import EMRJobRunner
from boto.s3.key import Key
import time
import sys
import ast
import boto

def check_sssp_completed():
    emr = EMRJobRunner()
    key = 'hw7/visited.txt'
    c = emr.fs.make_s3_conn()
    b = c.get_bucket('ucb-mids-mls-rajeshthallam') # substitute your bucket name here
    key_exists = b.get_key(key)
    
    if key_exists == None:
        return "-1"
    else:
        k = Key(b)
        k.key = key
        return k.get_contents_as_string()

def get_output(line):
    line = line.split('\t')

    node = line[0].strip('"')
    stripe = line[1].strip('"').split('|')
    edges = stripe[0]
    dist = stripe[1]
    path = stripe[2]
    state = stripe[3].replace('"', "")
        
    if state[0] == 'F':
        path = ast.literal_eval(path)
        words = [ lookup[w] for w in path ]
        end_word = lookup[node]
        shortest_path = ' -> '.join(words) + " -> " + end_word
        min_distance = str(dist)
            
        return [shortest_path, min_distance]
    
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

# iterate until shortest path from source to target is found
counter = 0
prev_counters = [{}]

while (1):
    counter += 1
    print "iteration #{}".format(counter)

    if counter == 1:
        s3_source = SOURCE
    else:
        s3_source = 's3://ucb-mids-mls-rajeshthallam/hw7/ssps/' + str(counter-1) + "/"
    
    s3_target = 's3://ucb-mids-mls-rajeshthallam/hw7/ssps/' + str(counter) + "/"
    
    print s3_source
    print s3_target
    
    mr_job = MRDijkstra(args=[s3_source, 
                          '--startnode', START_NODE, 
                          '--endnode', END_NODE, 
                          '--bucket', 'ucb-mids-mls-rajeshthallam',
                          '--runmode', 'emr',
                          '-r', 'emr',
                          '--pool-emr-job-flows',
                          '--max-hours-idle', '1',
                          '--output-dir', s3_target,
                          '--no-output',
                          '--no-strict-protocol'])

    with mr_job.make_runner() as runner: 
        runner.run()
    
    status = check_sssp_completed()
    if status != "-1":
        break
    else:
        print status
        
print get_output(status)
shortest_path, min_distance = get_output(status)

end_time = time.time()
print "Time taken to find shortest path = {:.2f} seconds".format(end_time - start_time)
print "shortest path in the synonyms from word '{}' to word '{}' is [ {} ] with distance of {}".format(START_WORD, END_WORD, shortest_path, min_distance)