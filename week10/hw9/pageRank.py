#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRPageRank(MRJob):

    def steps(self):
        return (
                [MRStep(mapper=self.m_pagerank_init)] +
                [MRStep(mapper = self.m_distribute,
                        reducer = self.r_combine_mass),
                MRStep(reducer = self.r_update_pagerank)
                ] * self.options.num_iterations
        )

    # set command line option to accept start and end node
    def configure_options(self):
        super(MRPageRank, self).configure_options()
        self.add_passthrough_option('--num-iterations', default=10, type=int, help='number of iterations to compute stable pagerank')
        self.add_passthrough_option('--N', default=None, type=float, help='total number of webpages (or nodes)')
        self.add_passthrough_option('--d', default=0.85, type=float, help='dampening factor')

    #    pagerank_init initializes each node with 1/|G|,
    #    where |G| is number of nodes in the entire graph
    #    (determined in a pre-processing phase)
    def m_pagerank_init(self, _, line):
        node, out_links = line.split('\t')
        pr = 1 / self.options.N
        yield node, out_links + "|" + str(pr)

    #   distribute mass to the outgoing links
    def m_distribute(self, node, pr_out_links):
        # parse input to read page rank of the outgoing links
        out_links, pr = map(ast.literal_eval, pr_out_links.split('|'))
        
        # vanilla page rank
        if len(out_links) > 0:
            pr_new = pr / len(out_links)

            for out_link in out_links:
                yield out_link, pr_new

        # if the node is for dangling (i.e. no outgoing link),
        # emit the loss to redistribute to all the incoming
        # links to the dangling node
        if len(out_links) == 0:
            yield 'dangling', pr

        # recover graph structure
        yield node, out_links

    # update pagerank by combining the mass
    def r_combine_mass(self, node, pr_out_links):

        # if the node is dangling, redistribute the loss
        # to all the nodes in the graph
        if node == 'dangling':
            loss = sum(pr_out_links)
            for n in range(1, int(self.options.N) + 1):
                yield str(n), loss
        # else combine the mass for the node
        else:
            M = 0
            out_links = {}

            for pr_out_link in pr_out_links:
                if type(pr_out_link) == dict:
                    out_links = pr_out_link
                elif type(pr_out_link) == float:
                    M += pr_out_link

            yield node, str(out_links) + "|" + str(M)
            
    def r_update_pagerank(self, node, pr_out_links):
        loss = 0.0
        pr = 0.0
        out_links = {}
        
        # teleportation factor
        a = 1 - self.options.d
        N = self.options.N
        
        for pr_out_link in pr_out_links:
            if type(pr_out_link) == float:
                loss = pr_out_link
            else:
                out_links, pr = map(ast.literal_eval, pr_out_link.split('|'))

        pr_new = a * (1/N) + (1-a) * (loss/N + float(pr))

        yield node, str(out_links) + "|" + str(round(pr_new, 5))

if __name__ == '__main__':
    MRPageRank.run()