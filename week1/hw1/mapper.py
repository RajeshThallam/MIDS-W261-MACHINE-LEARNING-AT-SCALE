#!/usr/bin/python
import traceback
import sys
import re

from collections import Counter

# read input parameters
data_file = sys.argv[1]
words = sys.argv[2]

try:
    search_all = 0

    if words == "*":
        search_all = 1
        word_list = []
    else:
        word_list = words.split() 
    
    with open (data_file, "r") as emails:
        for email in emails:
            # split email by tab (\t)
            mail = email.split('\t')
                
            # handle missing email content
            if len(mail) == 3:
                mail.append(mail[2])
                mail[2] = ""
            assert len(mail) == 4
    
            # email id
            email_id = mail[0]
            # spam/ham binary indicator
            is_spam = mail[1]
            # email content - remove special characters and punctuations
            #content = re.sub('[^A-Za-z0-9\s]+', '', mail[2] + " " +  mail[3])
            content = re.sub('[^A-Za-z0-9\s]+', '', mail[3])
            # count number of words
            content_wc = len(content.split())
    
            # find words with counts - works for single word or list of words
            if search_all == 1:
                hits = Counter(content.split())
            else:
                find_words = re.compile("|".join(r"\b%s\b" % w for w in word_list))
                hits = Counter(re.findall(find_words, content))

            hits = {k: v for k, v in hits.iteritems()}
            
            # emit tuple delimited by |
            # (spam ind, content word count, word hit counts)
            print "{} | {} | {} | {} | {}".format(email_id, is_spam, content_wc, word_list, hits)
except Exception: 
    traceback.print_exc()