date
#./FrequentBigrams.py \
#    s3://filtered-5grams -r emr \
#    --output-dir=s3://ucb-mids-mls-rajeshthallam/hw_5_4/bigrams \
#    --file s3://ucb-mids-mls-rajeshthallam/hw_5_3/most_frequent_unigrams/frequent_unigrams_10K.txt \
#    --no-output \
#    --no-strict-protocol
./FrequentBigrams.py \
    /home/rt/wrk/w261/hw5/data -r local \
    --file /home/rt/wrk/w261/hw5/output/frequent_unigrams_10K.txt \
    --no-strict-protocol 2>/dev/null > ./output/hw541_word_cooccurences.txt
#./FrequentBigrams.py s3://filtered-5grams -r emr -q --output-dir=s3://ucb-mids-mls-rajeshthallam/hw_5_4/bigrams --file /home/rt/wrk/w261/hw5/output/frequent_unigrams_10K.txt --no-output --no-strict-protocol
date
