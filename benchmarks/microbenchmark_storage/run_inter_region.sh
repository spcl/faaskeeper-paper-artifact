#!/bin/bash

python3 benchmark.py eu-central-1 ../../data/microbenchmark_storage/interregion_dynamodb 1000 dynamodb false 409550 > ../../data/microbenchmark_storage/interregion_dynamodb.log
python3 benchmark.py eu-central-1 ../../data/microbenchmark_storage/interregion_s3 1000 s3 false > ../../data/microbenchmark_storage/interregion_s3.log

