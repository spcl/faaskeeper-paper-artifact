#!/usr/bin/env python3

import glob
import sys

import pandas as pd

pubsub_fifo = pd.read_csv('../../data/microbenchmark_queues/gcp/pubsub_fifo/pubsub_fifo_2048.csv')
pubsub = pd.read_csv('../../data/microbenchmark_queues/gcp/pubsub/pubsub_2048.csv')
cloud_function = pd.read_csv('../../data/microbenchmark_queues/gcp/direct/cloud_function_2048.csv')

pubsub_subset = pubsub.loc[(pubsub_fifo['type'] == 'invocation') & (pubsub_fifo['is_cold'] == False)]
pubsub_fifo_subset = pubsub_fifo.loc[(pubsub_fifo['type'] == 'invocation') & (pubsub_fifo['is_cold'] == False)]
cloud_function = cloud_function.loc[(cloud_function['type'] == 'invocation') & (cloud_function['is_cold'] == False)]

print(pubsub.groupby(['memory', 'size']).count())
print(pubsub_fifo_subset.groupby(['memory', 'size']).count())
print(cloud_function.groupby(['memory', 'size']).count())

for name, val, arg in [('Min', 'min', None), ('p50', 'median', None), ('p95', 'quantile', .95), ('p99', 'quantile', .99), ('Max', 'max',  None)]:

    vals = []
    for data in [cloud_function, pubsub_subset, pubsub_fifo_subset]:
        sqs_group = data.groupby(['memory', 'size'], as_index=False)['data']
        func = getattr(sqs_group, val)
        if arg is not None:
            res = func(arg)
        else:
            res = func()
        for size in (64, 65536):
            vals.append(res.loc[res['size'] == size].values[0][2] *1000.0)
    #print(vals)
    st = r'\textbf{' + name + '}'
    for val in vals:
        st = st + '\t&\t' + str(round(val,2))
    st = st + r'\\'
    print(st)

