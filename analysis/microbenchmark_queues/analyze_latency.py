#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv('../../data/microbenchmark_queues/sqs_fifo.csv')
print(df)

df_subset = df.loc[df['type'] == 'invocation']
print(df_subset.groupby(['memory', 'size'])['data'].median() / 1000.0)
print(df_subset.groupby(['memory', 'size'])['data'].quantile(.75) / 1000.0)
print(df_subset.groupby(['memory', 'size'])['data'].quantile(.90) / 1000.0)
print(df_subset.groupby(['memory', 'size'])['data'].quantile(.95) / 1000.0)
print(df_subset.groupby(['memory', 'size'])['data'].quantile(.99) / 1000.0)
