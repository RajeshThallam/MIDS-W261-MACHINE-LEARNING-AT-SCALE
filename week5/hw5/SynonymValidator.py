#!/usr/bin/python
import nltk
from nltk.corpus import wordnet as wn
from itertools import combinations
import sys, re

TOP_10K = './output/frequent_unigrams_10K.txt'
EUCLID = './output/euclid_top_1K.txt'
COSINE = './output/cosine_top_1K.txt'

def synonyms(string):
    syndict = {}
    for i,j in enumerate(wn.synsets(string)):
        syns = j.lemma_names()
        for syn in syns:
            syndict.setdefault(syn,1)
    return syndict.keys()

def synonym_pairs():
    # create pairs with the full collection of unique 
    # synonym (word1,word2) pairs subject to top 10K words
    wordList = []
    for line in open(TOP_10K).read().strip().split('\n'):
        word = line.split('\t')[0].strip(' "')
        wordList.append(word)

    pairs = {}
    for word in wordList:
        syns = synonyms(word)
        keepSyns = []
        for syn in syns:
            syn = str(re.sub("_"," ",syn))
            if syn != word:
                if syn in wordList:
                    keepSyns.append(syn)
        for syn in keepSyns:
            pair = ",".join(sorted([word,syn]))
            pairs[pair] = 1

    return pairs

def calculate_measures(similar_words, threshold):
    rownum = 0
    hits = []
    
    synonyms = synonym_pairs()

    for line in open(similar_words).read().strip().split('\n'):
        # pair, distance
        p, d = line.strip().split('\t')
        p = ast.literal_eval(p)
        d = float(d)
        
        if p[0] in synonym_pairs(p[1]) or p[1] in synonym_pairs(p[0]):
            hits.append(1)
        else:
            hits.append(0)

        rownum += 1

    # assume words below threshold are hits
    predictions = [1]*threshold + [0]*(rownum-threshold)

    # determine measures
    tp = 0
    fn = 0
    fp = 0
    tn = 0

    for i in range(len(predictions)):
        # true positives
        if hits[i] == 1 and predictions[i] == 1:
            tp += 1
        # true negatives
        elif hits[i] == 0 and predictions[i] == 0:
            tn += 1
        # false negatives
        elif hits[i] == 1 and predictions[i] == 0:
            fn += 1
        # false positives
        else:
            fp += 1

    accuracy = float(tp + tn) / len(predictions)
    recall = float(tp) / float(tp + fn)
    precision = float(tp) / float(tp + fp)

    print "Accuracy: {}".format(accuracy)
    print "Precision: {}".format(precision)
    print "Recall: {}".format(recall)
    
    print "F1 Score: {}".format(2 * (precision*recall) / (precision + recall))

if __name__ == '__main__':
    threshold = 1000
    # euclidean
    calculate_measures(EUCLID, threshold)

    # cosine similarity
    calculate_measures(COSINE, threshold)
