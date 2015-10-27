#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.emr import EMRJobRunner
from boto.s3.key import Key
import ast
import sys
import boto

class MRDijkstra(MRJob):    
    # set command line option to accept start and end node
    def configure_options(self):
        super(MRDijkstra, self).configure_options()

        self.add_passthrough_option('--startnode', default='1', type=str, help='starting node for single source shortest path')
        self.add_passthrough_option('--endnode', default=None, type=str, help='target node to be visited')
        self.add_passthrough_option('--bucket', default=None, type=str, help='bucket to write secondary output')
        self.add_passthrough_option('--runmode', default=None, type=str, help='local or emr')

    # initialize mapper with start node
    def mapper_init(self):
        self.startnode = self.options.startnode

    # for each node emit adjacency list with path and distance
    # (node, [edges, distance, path, state]))
    def mapper(self, _, line):
        # parse input
        line = line.strip()
        data = line.split('\t')
        values = data[1].strip('"').split("|")
        
        # init variables
        node = None
        edges = None
        path = list()
        dist = sys.maxint
        state = None
        
        # parse the input for first iteration
        if len(values) >= 1:
            node = data[0].strip('"')
            edges = ast.literal_eval(values[0])
        # parse the input for subsequent iterations 
        if len(values) == 4:
            dist = int(values[1])
            path = ast.literal_eval(values[2])
            state = values[3]
        
        # for first pass set start node to q status
        if state == None and self.startnode == node:
            state = 'q'
            dist = 0
        
        # for frontier nodes emit node with its neighbors
        if state == 'q':
            yield node, (edges, dist, path, 'v')
            path.append(node)

            for n in edges.iterkeys():
                yield n, (None, dist + 1, path, 'q')
        # else emit the current node
        else:
            yield node, (edges, dist, path, state)
          
    # initialize reducer
    def reducer_init(self):
        self.endnode = self.options.endnode     # read end node from command line
        self.target_reached = -1                # set if target node reached
        self.target_path = ""                   # set the path from source to target
        self.s3bucket = self.options.bucket     # intermediate s3 bucket to check for terminaton condition
        self.runmode = self.options.runmode     # read run mode - local or emr

    # reducers select the minimum of the distances for each destination node. 
    # each iteration of MapReduce job of the algorithm expands the "search 
    # frontier" by one hop.
    # if the node is set visited yield shortest distance and path
    def reducer(self, key, values):
        # initialize each node
        edges = {}
        dist = list()
        state = list()
        path = list()
        f_state = ''

        for v in values:
            v_edges = v[0]
            v_dist = v[1]
            v_path = v[2]
            v_state = v[3]

            if v_state == 'v':
                edges = v_edges
                dist.append(v_dist)
                path = v_path
                state.append(v_state)
                break
            
            # look for shortest distance
            dist.append(v_dist)
            state.append(v_state)

            # recover graph structure
            if v_edges != None:
                edges = v_edges
            if v_path != None and len(v_path) > 0:
                path = v_path
        
        # update shortest distance
        Dmin = min(dist)
        
        # keep track of pending and visited counters
        # set target reached when end node is reached
        if 'v' in state:
            f_state = 'v'
            if self.endnode != None and key.strip('"') == self.endnode:
                self.increment_counter('group', 'visited', 0)
                self.target_reached = 1
                self.target_path = str(key) + "\t" + str(edges) + '|' + str(Dmin) + '|' + str(path) + '|' + "F"
        elif 'q' in state:
            f_state = 'q'
            self.increment_counter('group', 'pending', 1)
        else:
            f_state = 'u'
            self.increment_counter('group', 'pending', 1)
            
        yield key, str(edges) + '|' + str(Dmin) + '|' + str(path) + '|' + f_state

    # if the target is reached, record the path and shortest 
    # distance on external S3 file if run on emr mode
    # if run in local, stream the output
    def reducer_final(self):
        if self.target_reached == 1:
            sys.stderr.write('Target reached')
            if self.runmode == 'emr':
                sys.stderr.write(self.target_path)
                s3_key = 'hw7/visited.txt'
                emr = EMRJobRunner()
                c = emr.fs.make_s3_conn()
                b = c.get_bucket(self.s3bucket)
                k = Key(b)
                k.key = s3_key
                k.set_contents_from_string(self.target_path)
                #self.write_to_s3(self.options.bucket, s3_key, self.target_path)
            else:
                yield self.target_path.split('\t')[0], self.target_path.split('\t')[1]
            
    # write to s3 bucket key with the string
    def write_to_s3(bucket, key, string):
        emr = EMRJobRunner()
        c = emr.fs.make_s3_conn()
        b = c.get_bucket(bucket)
        k = Key(b)
        k.key = key
        k.set_contents_from_string(string)
            
if __name__ == '__main__':
    MRDijkstra.run()