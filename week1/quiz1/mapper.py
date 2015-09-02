#!/usr/bin/python
import sys
import re
count = 0
WORD_RE = re.compile(r"[\w']+")
filename = sys.argv[2]
findword = sys.argv[1]
with open (filename, "r") as myfile:
    # case insensitive search for the word
    for line in myfile:
        count = (count + 1) if re.search("\\b" + findword + "\\b", line, re.IGNORECASE) else count
print count