#!/usr/bin/python
import traceback
import math
import sys
import ast

from collections import Counter

# read input parameters
files = sys.argv[1:]

try:
    spam_count = 0
    ham_count = 0
    spam_all_wc = 0
    ham_all_wc = 0
    spam_term_wc = {}
    ham_term_wc = {}
    pr_word_given_spam = {}
    pr_word_given_ham = {}

    # read each mapper output
    for f in files:
        with open (f, "r") as emails:
            for email in emails:
                # parse mapper output
                mail = email.split(" | ")
                # read spam/ham indicator, content word count, 
                is_spam = int(mail[1])
                content_wc = int(mail[2])
                vocab = ast.literal_eval(mail[3])
                hits = ast.literal_eval(mail[4])

                # capture counts required for naive bayes probabilities
                if is_spam:
                    # spam mail count
                    spam_count += 1
                    # term count when spam
                    spam_term_wc = dict(Counter(hits) + Counter(spam_term_wc))
                    # all word count when spam
                    spam_all_wc += content_wc
                else:
                    # ham email count
                    ham_count += 1
                    # term count when ham
                    ham_term_wc = dict(Counter(hits) + Counter(ham_term_wc))
                    # all word count when ham
                    ham_all_wc += content_wc

    # vocab size
    vocab = dict(Counter(vocab) + Counter(spam_term_wc) + Counter(ham_term_wc))
    V = len(vocab) * 1.0
    print "vocab size = {}".format(V)
                        
    # calculate priors
    pr_spam_prior = (1.0 * spam_count) / (spam_count + ham_count)
    pr_ham_prior = (1.0 - pr_spam_prior)
    pr_spam_prior = math.log10(pr_spam_prior)
    pr_ham_prior = math.log10(pr_ham_prior)
    
    # calculate conditional probabilites with laplace smoothing = 1
    # pr_word_given_class = ( count(w, c) + 1 ) / (count(c) + 1 * |V|)
    for word in vocab:
        pr_word_given_spam[word] = math.log10((spam_term_wc.get(word, 0) + 1.0) / (spam_all_wc + V))
        pr_word_given_ham[word] = math.log10((ham_term_wc.get(word, 0) + 1.0) / (ham_all_wc + V))
    
    print "/*log probabilities*/"
    print "pr_spam_prior = {}".format(pr_spam_prior)
    print "pr_ham_prior = {}".format(pr_ham_prior)
    
    print "\n"
    print "{0: <50} | {1} | {2}".format("ID", "TRUTH", "CLASS")
    print "{0: <50}-+-{1}-+-{2}".format("-" * 50, "-" * 7, "-" * 10)

    # spam/ham prediction using Multinomial Naive Bayes priors and conditional probabilities
    accuracy = []
    for f in files:
        with open (f, "r") as emails:
            for email in emails:
                # initialize
                word_count = 0
                pred_is_spam = 0
                pr_spam = pr_spam_prior
                pr_ham = pr_ham_prior

                # parse mapper output
                mail = email.split(" | ")
                email_id = mail[0]
                is_spam = int(mail[1])
                hits = ast.literal_eval(mail[4])

                # number of search words
                word_count = sum(hits.values())

                # probability for each class for a given email
                # argmax [ log P(C) + sum( P(Wi|C) ) ]
                for word in vocab:
                    pr_spam += (pr_word_given_spam.get(word, 0) * hits.get(word, 0))
                    pr_ham += (pr_word_given_ham.get(word, 0) * hits.get(word, 0))

                # predict based on maximum likelihood
                if pr_spam > pr_ham: 
                    pred_is_spam = 1

                # calculate accuracy
                accuracy.append(pred_is_spam==is_spam)
                
                print '{0:<50} | {1:<7} | {2:<10}'.format(email_id, is_spam, pred_is_spam)

    print "\n"
    print "/*accuracy*/"
    print "accuracy = {:.2f}".format(sum(accuracy) / float(len(accuracy)))
    
except Exception: 
    traceback.print_exc()