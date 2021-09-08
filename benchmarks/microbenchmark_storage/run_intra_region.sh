#!/bin/bash

python3 benchmark.py us-east-1 ../../data/microbenchmark_storage/intraregion_dynamodb 1000 dynamodb false 409550
python3 benchmark.py us-east-1 ../../data/microbenchmark_storage/intraregion_s3 1000 s3 false

