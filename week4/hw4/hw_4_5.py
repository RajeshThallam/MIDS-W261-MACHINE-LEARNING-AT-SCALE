import numpy as np
import sys
from Kmeans import MRKmeans, stop_criterion

# initialize variables
SOURCE    = "topUsers_Apr-Jul_2014_1000-words.txt"
SUMMARY   = "topUsers_Apr-Jul_2014_1000-words_summaries.txt"
CENTROIDS = "/tmp/centroids"
THRESHOLD = 0.001

# set the randomizer seed so results are the same each time.
np.random.seed(0)

# define mrjob runner
mr_job = MRKmeans(args=[SOURCE])

# validate driver inputs - K and distribution type
if len(sys.argv) != 3:
    print "Invalid number of arguments. Pass k (cluster size) and centroid distribution type (uniform, perturbed, normal)"
    sys.exit(1)

k = sys.argv[1]
try:
    k = int(k)
except:
    raise TypeError("Invalid k. k must be an integer")

distr_type = sys.argv[2]
if distr_type not in ['uniform', 'perturbed', 'trained']:
    print "Invalid centroid distribution type. Type should be uniform, perturbed or trained."
    sys.exit(1)

# generate initial centroids based on initialization and parameterization

# (A) uniform random centroid-distributions over the 1000 words
if distr_type == 'uniform':
    # uniform random distribution
    d = np.random.uniform(size=[k, 1000])
    # total
    t = np.sum(d, axis=1)
    # normalize distribution
    centroid_points = np.true_divide(d.T, t).T

# (B) & (C) perturbation-centroids, randomly perturbed from the aggregated (user-wide) distribution
# perturbation is assumed as noise
elif distr_type == 'perturbed':
    # read ALL_CODES line in the aggregated summary file
    aggregated = open(SUMMARY, 'r').readlines()[1].strip().split(',')
    
    # normalize
    total = int(aggregated[2])
    summaries = [float(wc) / total for wc in aggregated[3:]]
    
    # normalized perturbation on the aggregated word counts
    perturbation = summaries + ( np.random.sample(size = (k, 1000)) / 1000 )
    
    # normalize
    t = np.sum(perturbation, axis=1)
    centroid_points = np.true_divide(perturbation.T, t).T

# (D) "trained" centroids, determined by the sums across the classes
elif distr_type == 'trained':
    summaries = []

    # use trained rows in the aggregated file after
    for line in open(SUMMARY).readlines()[2:]:
        # read trained summary counts
        aggregated = line.strip().split(',')
        
        # normalize
        total = int(aggregated[2])
        summaries.append([float(wc) / total for wc in aggregated[3:]])

    centroid_points = np.array(summaries)

# write initial centroids to file
with open(CENTROIDS, 'w+') as f:
    f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
f.close()

# update centroids iteratively
i = 1
while(1):
    # save previous centoids to check convergency
    centroid_points_old = centroid_points

    with mr_job.make_runner() as runner: 
        #print "running iteration" + str(i) + ":"
        runner.run()
        centroid_points = []
        clusters = {}
        
        # stream_output: get access of the output 
        for line in runner.stream_output():
            key, value =  mr_job.parse_output_line(line)
            centroid, codes = value
            centroid_points.append(centroid)
            clusters[key] = codes

    if(stop_criterion(centroid_points_old, centroid_points, THRESHOLD)):
        # display statistics
        print "cluster distribution"
        print "-" * 80
        print "iteration # {}".format(i)
        codes = { 0:'Human', 1:'Cyborg', 2:'Robot', 3:'Spammer' }
        max_class = {}
        print "-" * 80
        print "{0:>5} |{1:>12} (%) |{2:>12} (%) |{3:>12} (%) |{4:>12} (%)".format("k", "Human", "Cyborg", "Robot", "Spammer")
        print "-" * 80
        for cluster_id, cluster in clusters.iteritems():
            total = sum(cluster.values())
            print "{0:>5} | {1:>5} ({2:6.2f}%) | {3:>5} ({4:6.2f}%) | {5:>5} ({6:6.2f}%) | {7:>5} ({8:6.2f}%)".format(
                cluster_id, 
                cluster.get('0', 0),
                float(cluster.get('0', 0))/total*100,
                cluster.get('1', 0),
                float(cluster.get('1', 0))/total*100,
                cluster.get('2', 0),
                float(cluster.get('2', 0))/total*100,
                cluster.get('3', 0),
                float(cluster.get('3', 0))/total*100
            )
            max_class[cluster_id] = max(cluster.values())
        purity = sum(max_class.values())/1000.0*100
        print "-" * 80
        print "purity = {0:0.2f}%".format(purity)
        print "-" * 80
        break

    # write new centroids to file
    with open(CENTROIDS, 'w') as f:
        for centroid in centroid_points:
            f.writelines(','.join(map(str, centroid)) + '\n')
    f.close()
    i += 1