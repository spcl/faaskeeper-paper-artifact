#!/bin/bash

script -e -c "python3 benchmark.py --region us-east-1 --output-prefix ../../data/microbenchmark_storage/intraregion_dynamodb --repetitions 1000 --storage-type dynamodb --max-size 409550"  -f ../../data/microbenchmark_storage/intraregion_dynamodb.log
script -e -c "python3 benchmark.py --region us-east-1 --output-prefix ../../data/microbenchmark_storage/intraregion_s3 --repetitions 1000 --storage-type s3 --storage-suffix intra" -f ../../data/microbenchmark_storage/intraregion_s3.log

