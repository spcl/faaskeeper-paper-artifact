#!/usr/bin/env python3

import glob
import sys

import pandas as pd


#df = pd.read_csv('../../data/microbenchmark_queues/sqs_fifo.csv')
sqs = pd.read_csv('../../data/microbenchmark_queues/sqs/sqs_2048.csv')
sqs_fifo = pd.read_csv('../../data/microbenchmark_queues/sqs_fifo/sqs_fifo_2048.csv')
dynamo = pd.read_csv('../../data/microbenchmark_queues/dynamo/dynamodb_2048.csv')
lambda_ = pd.read_csv('../../data/microbenchmark_queues/direct/lambda_2048.csv')
lambda_cold_ = pd.read_csv('../../data/microbenchmark_queues/direct/lambda_cold_2048.csv')

sqs_subset = sqs.loc[(sqs['type'] == 'invocation') & (sqs['is_cold'] == False)]
sqs_fifo_subset = sqs_fifo.loc[(sqs_fifo['type'] == 'invocation') & (sqs_fifo['is_cold'] == False)]
dynamo = dynamo.loc[(dynamo['type'] == 'invocation') & (dynamo['is_cold'] == False)]
lambda_ = lambda_.loc[(lambda_['type'] == 'invocation') & (lambda_['is_cold'] == False)]
lambda_cold_ = lambda_cold_.loc[(lambda_cold_['type'] == 'invocation')]

print(sqs_subset.groupby(['memory', 'size']).count())
print(sqs_fifo_subset.groupby(['memory', 'size']).count())
print(dynamo.groupby(['memory', 'size']).count())
print(lambda_.groupby(['memory', 'size']).count())
print(lambda_cold_.groupby(['memory', 'size']).count())

for name, val, arg in [('Min', 'min', None), ('p50', 'median', None), ('p95', 'quantile', .95), ('p99', 'quantile', .99), ('Max', 'max',  None)]:

    vals = []
    for data in [lambda_, sqs_subset, sqs_fifo_subset, dynamo]:
        sqs_group = data.groupby(['memory', 'size'], as_index=False)['data']
        func = getattr(sqs_group, val)
        if arg is not None:
            res = func(arg)
        else:
            res = func()
        #print(res)
        for size in (64, 65536):
            vals.append(res.loc[res['size'] == size].values[0][2] *1000.0)
    #print(vals)
    st = r'\textbf{' + name + '}'
    for val in vals:
        st = st + '\t&\t' + str(round(val,2))
    st = st + r'\\'
    print(st)


print("Difference Lambda TCP Warm vs Lambda TCP Cold")
for name, val, arg in [('Min', 'min', None), ('p50', 'median', None), ('p95', 'quantile', .95), ('p99', 'quantile', .99), ('Max', 'max',  None)]:
    warm = lambda_.groupby(['memory', 'size'], as_index=False)['data']
    cold = lambda_cold_.groupby(['memory', 'size'], as_index=False)['data']
    func = getattr(warm, val)
    if arg is not None:
        res = func(arg)
    else:
        res = func()
    func2 = getattr(cold, val)
    if arg is not None:
        res2 = func2(arg)
    else:
        res2 = func2()
    for size in (64, 1024, 131072):
        print(name, size, res2.loc[res2['size'] == size].values[0][2] *1000.0 - res.loc[res['size'] == size].values[0][2] *1000.0)

print("Measured RTT of TCP connection")
lambda_ = pd.read_csv('../../data/microbenchmark_queues/direct/lambda_2048.csv')
lambda_ = lambda_.loc[lambda_['type'] == 'rtt']
for name, val, arg in [('Min', 'min', None), ('p50', 'median', None), ('p95', 'quantile', .95), ('p99', 'quantile', .99), ('Max', 'max',  None)]:
    sqs_group = lambda_.groupby(['memory'], as_index=False)['data']
    func = getattr(sqs_group, val)
    if arg is not None:
        res = func(arg)
    else:
        res = func()
    print(name, res * 1000.0)
#
#142      \textbf{Min}            &  &  6.08&  5.27  &  6.08  &  8.34  &  11.5  &  59.19&  5.27  &  6.08\\                                                     
#143      \textbf{p50}            &  5.27  &  6.08&  5.27  &  6.08  &  8.34  &  11.5  &  59.19&  6.13  &  6.8 \\
#        144      \textbf{p95}            &  5.27  &  6.08&  5.27  &  6.08  &  8.34  &  11.5  &  59.19&  7.82  &  67.16 \\
#        145      \textbf{p99}            &  5.27  &  6.08&  5.27  &  6.08  &  8.34  &  11.5  &  59.19&  7.82  &  67.16 \\
#        146      \textbf{Max}            &  5.27  &  6.08&  5.27  &  6.08  &  8.34  &  11.5  &  59.19&  7.82  &  67.16 \\



