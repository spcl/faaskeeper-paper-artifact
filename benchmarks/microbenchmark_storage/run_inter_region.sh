#!/bin/bash

#script -e -c "python3 benchmark.py --region eu-central-1 --output-prefix ../../data/microbenchmark_storage/interregion_dynamodb --repetitions 1000 --storage-type dynamodb --max-size 409550"  -f ../../data/microbenchmark_storage/interregion_dynamodb.log
script -e -c "python3 benchmark.py --region eu-central-1 --output-prefix ../../data/microbenchmark_storage/interregion_s3 --repetitions 1000 --storage-type s3 --storage-suffix inter" -f ../../data/microbenchmark_storage/interregion_s3.log

