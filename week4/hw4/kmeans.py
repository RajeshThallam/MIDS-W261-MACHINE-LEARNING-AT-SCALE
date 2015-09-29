import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep

CENTROIDS="/tmp/centroids"

# find the nearest centroid for data point 
def MinDist(data_point, centroid_points):
    # calculate euclidean distance
    euclidean_distance = np.sum((data_point - centroid_points)**2, axis = 1)
    # get the nearest centroid for each instance
    minidx = np.argmin(euclidean_distance)
    return minidx

# check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new, T):
    return np.alltrue(abs(np.array(centroid_points_new) - np.array(centroid_points_old)) <= T)

class MRKmeans(MRJob):
        
    centroid_points=np.array([])
    
    # define mrjob steps
    def steps(self):
        return [
            MRStep(
                mapper_init = self.mapper_init,
                mapper=self.mapper,
                combiner = self.combiner,
                reducer=self.reducer
            )
        ]

    # load centroids from file
    def mapper_init(self):
        self.centroid_points = np.loadtxt(CENTROIDS, delimiter=',')
    
    # load data and output the nearest centroid index and data point 
    # returns key = nearest centroid, values = tuple(features, class:1)
    def mapper(self, _, line):
        terms = line.strip().split(',')
        userid = terms[0]
        code = int(terms[1])
        total = int(terms[2])
        features = np.array([float(x) / total  for x in terms[3:]])

        # key    = centroid
        # values = tuple(features, code:1)
        yield int(MinDist(features, self.centroid_points)), (list(features), {code:1})
   
    # combine sum of data points locally
    def combiner(self, idx, inputdata):
        combine_features = None
        combine_codes = {}

        for features, code in inputdata:
            features = np.array(features)
            
            # local aggregate of features
            if combine_features is None:
                combine_features = np.zeros(features.size)
            combine_features += features

            # count number of codes
            for k, v in code.iteritems():
                combine_codes[k] = combine_codes.get(k, 0) + v

        yield idx, (list(combine_features), combine_codes)

    # aggregate sum for each cluster and then calculate the new centroids
    def reducer(self, idx, inputdata):
        combine_features = None
        combine_codes = {}
        
        for features, code in inputdata:
            features = np.array(features)

            # local aggregate of features
            if combine_features is None:
                combine_features = np.zeros(features.size)
            combine_features += features

            # count number of codes
            for k, v in code.iteritems():
                combine_codes[k] = combine_codes.get(k, 0) + v

        # new centroids
        centroids = combine_features / sum(combine_codes.values())

        yield idx, (list(centroids), combine_codes)

if __name__ == '__main__':
    MRKmeans.run()