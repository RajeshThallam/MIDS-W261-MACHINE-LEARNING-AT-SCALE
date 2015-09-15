#!/usr/bin/python
import traceback
import sys
import re

# read input parameters
find_word = sys.argv[1]

try:    
    for email in sys.stdin:
        # split email by tab (\t)
        mail = email.split('\t')
            
        # handle missing email content
        if len(mail) == 3:
            mail.append(mail[2])
            mail[2] = ""
        assert len(mail) == 4

        # email id
        email_id = mail[0]
        # email content - remove special characters and punctuations
        content = re.sub('[^A-Za-z0-9\s]+', '', mail[2] + " " +  mail[3])

        # find word with counts
        for word in content.split():
            if word == find_word:
                print '{}\t{}'.format(word, 1)
except Exception: 
    traceback.print_exc()