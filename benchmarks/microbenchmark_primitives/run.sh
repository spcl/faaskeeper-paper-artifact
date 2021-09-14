#!/bin/bash

script -e -c "python3 benchmark.py --region us-east-1 --output-prefix ../../data/microbenchmark_primitives/result --repetitions 1000 --max-size 409550"  -f ../../data/microbenchmark_primitives/result.log

