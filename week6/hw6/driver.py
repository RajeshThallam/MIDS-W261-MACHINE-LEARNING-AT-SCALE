#!/usr/bin/python
from GradientDescentWOLS import MRJobGradientDescentWOLS
import numpy as np
import sys
import time

start_time = time.time()

learning_rate = 0.1
stop_criteria = 0.00000005

# create a mrjob instance for batch gradient descent update over all data
mr_job = MRJobGradientDescentWOLS(args=['LinearRegression.csv', '--file', 'weights.txt', '--no-strict-protocol'])

# generate random values as inital weights
weights = np.array([np.random.uniform(-3,3), np.random.uniform(-3,3)])

# write the weights to the files
with open('weights.txt', 'w+') as f:
    f.writelines(','.join(str(j) for j in weights))

# update centroids iteratively
i = 0

# write coefficients for plotting
f_coeff = open(output_fname,'w')
    
while(1):
    coeff = "iteration={}\tweights=[{}, {}]".format(i, weights[0], weights[1])
    print coeff
    f_coeff.write(coeff + '\n')

    #Save weights from previous iteration
    weights_old = weights

    with mr_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output 
        for line in runner.stream_output():
            # value is the gradient value
            key,value =  mr_job.parse_output_line(line)
            # Update weights
            weights = weights - learning_rate*np.array(value)
    i = i + 1
    
    # Write the updated weights to file 
    with open('weights.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in weights))

    # Stop if weights get converged
    if(sum((weights_old-weights)**2) < stop_criteria):
        break

f_coeff.close()
        
print "Final weights\n"
print weights

end_time = time.time()
print "Time taken to determine gradient descent of weighted linear regression model = {:.2f} seconds".format(end_time - start_time)