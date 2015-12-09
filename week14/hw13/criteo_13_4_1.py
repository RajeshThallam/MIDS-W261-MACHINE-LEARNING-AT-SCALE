from collections import defaultdict
import hashlib
import sys
from math import log, exp
from pyspark import SparkContext
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics

def hashFunction(numBuckets, rawFeats, printMapping=False):
    """Calculate a feature dictionary for an observation's features based on hashing.

    Note:
        Use printMapping=True for debug purposes and to better understand how the hashing works.

    Args:
        numBuckets (int): Number of buckets to use as features.
        rawFeats (list of (int, str)): A list of features for an observation.  Represented as
            (featureID, value) tuples.
        printMapping (bool, optional): If true, the mappings of featureString to index will be
            printed.

    Returns:
        dict of int to float:  The keys will be integers which represent the buckets that the
            features have been hashed to.  The value for a given key will contain the count of the
            (featureID, value) tuples that have hashed to that key.
    """
    mapping = {}
    for ind, category in rawFeats:
        featureString = category + str(ind)
        mapping[featureString] = int(int(hashlib.md5(featureString).hexdigest(), 16) % numBuckets)
    if(printMapping): print mapping
    sparseFeatures = defaultdict(float)
    for bucket in mapping.values():
        sparseFeatures[bucket] += 1.0
    return dict(sparseFeatures)

def parseHashPoint(point, numBuckets):
    """Create a LabeledPoint for this observation using hashing.

    Args:
        point (str): A comma separated string where the first value is the label and the rest are
            features.
        numBuckets: The number of buckets to hash to.

    Returns:
        LabeledPoint: A LabeledPoint with a label (0.0 or 1.0) and a SparseVector of hashed
            features.
    """
    parsedPoints = parsePoint(point)
    items = point.split(',')
    label = items[0]
    features = hashFunction(numBuckets, parsedPoints, printMapping=False)
    return LabeledPoint(label, SparseVector(numBuckets, features))

def parsePoint(point):
    """Converts a comma separated string into a list of (featureID, value) tuples.

    Note:
        featureIDs should start at 0 and increase to the number of features - 1.

    Args:
        point (str): A comma separated string where the first value is the label and the rest
            are features.

    Returns:
        list: A list of (featureID, value) tuples.
    """
    return [(i, item) for i, item in enumerate(point.split(',')[1:])]
    
def computeLogLoss(p, y):
    """Calculates the value of log loss for a given probabilty and label.

    Note:
        log(0) is undefined, so when p is 0 we need to add a small value (epsilon) to it
        and when p is 1 we need to subtract a small value (epsilon) from it.

    Args:
        p (float): A probabilty between 0 and 1.
        y (int): A label.  Takes on the values 0 and 1.

    Returns:
        float: The log loss value.
    """
    epsilon = 10e-12
    if p == 0:
        p = p + epsilon
    if p == 1:
        p = p - epsilon
    return -(y * log(p) + (1-y) * log(1-p))

def getP(x, w, intercept):
    """Calculate the probability for an observation given a set of weights and intercept.

    Note:
        We'll bound our raw prediction between 20 and -20 for numerical purposes.

    Args:
        x (SparseVector): A vector with values of 1.0 for features that exist in this
            observation and 0.0 otherwise.
        w (DenseVector): A vector of weights (betas) for the model.
        intercept (float): The model's intercept.

    Returns:
        float: A probability between 0 and 1.
    """
    rawPrediction = x.dot(w) + intercept

    # Bound the raw prediction value
    rawPrediction = min(rawPrediction, 20)
    rawPrediction = max(rawPrediction, -20)
    return 1 / (1 + exp(-rawPrediction))

def evaluateResults(model, data):
    """Calculates the log loss for the data given the model.

    Args:
        model (LogisticRegressionModel): A trained logistic regression model.
        data (RDD of LabeledPoint): Labels and features for each observation.

    Returns:
        float: Log loss for the data.
    """
    return data.map(lambda x: computeLogLoss(getP(x.features, model.weights, model.intercept), x.label)).sum() / data.count()

def evaluateMetrics(model, data, label):
    labelsAndScores = data.map(lambda lp:
                            (lp.label, getP(lp.features, model.weights, model.intercept)))
    
    auc = BinaryClassificationMetrics(labelsAndScores).areaUnderROC
    log_loss = evaluateResults(model, data)

    sys.stderr.write('\n LogLoss {0} = {1}'.format(label, log_loss))
    sys.stderr.write('\n AUC {0} = {1}\n'.format(label, auc))
    
    return (label, log_loss, auc)

if __name__ == '__main__':  
    # Initialize the spark context.
    sc = SparkContext(appName="CriteoBaseline")

    # =========================
    # read raw criteo data set
    # =========================
    rawTrainData = (sc
               .textFile(sys.argv[1], 2)
               .map(lambda x: x.replace('\t', ','))
               .cache() )# work with either ',' or '\t' separated data
    print rawTrainData.take(1)
    
    rawTestData = (sc
               .textFile(sys.argv[2], 2)
               .map(lambda x: x.replace('\t', ','))
               .cache() )# work with either ',' or '\t' separated data
    print rawTestData.take(1)
    
    rawValidationData = (sc
               .textFile(sys.argv[3], 2)
               .map(lambda x: x.replace('\t', ','))
               .cache() )# work with either ',' or '\t' separated data
    print rawValidationData.take(1)

    # ===================================================
    # split into train, validation and test data set
    # ===================================================
    #weights = [.8, .1, .1]
    #seed = 42
    # Use randomSplit with weights and seed
    #rawTrainData, rawValidationData, rawTestData = rawData.randomSplit(weights, seed)
    # Cache the data
    #rawTrainData.cache()
    #rawValidationData.cache()
    #rawTestData.cache()

    nTrain = rawTrainData.count()
    nVal = rawValidationData.count()
    nTest = rawTestData.count()
    print nTrain, nVal, nTest, nTrain + nVal + nTest

    # ===================================================
    # create hash features
    # ===================================================
    numBucketsCTR = 1000    # number of hash buckets

    hashTrainData = rawTrainData.map(lambda x: parseHashPoint(x, numBucketsCTR))
    hashTrainData.cache()
    hashValidationData = rawValidationData.map(lambda x: parseHashPoint(x, numBucketsCTR))
    hashValidationData.cache()
    hashTestData = rawTestData.map(lambda x: parseHashPoint(x, numBucketsCTR))
    hashTestData.cache()

    # ===================================================
    # train logistic regression model
    # ===================================================    
    numIters = 100
    stepSize = 10.
    regParam = 0. # no regularization
    regType = 'l2'
    includeIntercept = True

    model = LogisticRegressionWithSGD.train(hashTrainData, 
                                            iterations=numIters, 
                                            step=stepSize, 
                                            regParam=regParam, 
                                            regType=regType, 
                                            intercept=includeIntercept) 
    sortedWeights = sorted(model.weights)

    sys.stderr.write('\n Model Intercept: {0}'.format(model.intercept))
    sys.stderr.write('\n Model Weights (Top 5): {0}\n'.format(sortedWeights[:5]))
    
    l_metrics = []
    
    l_metrics.append(evaluateMetrics(model, hashTrainData, 'TRAIN'))
    l_metrics.append(evaluateMetrics(model, hashValidationData, 'VALIDATE'))
    l_metrics.append(evaluateMetrics(model, hashTestData, 'TEST'))
    
    sc.parallelize(l_metrics).saveAsTextFile(sys.argv[4])
    
    sc.stop()