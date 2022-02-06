#!/usr/bin/env python3

import pandas as pd
import numpy as np
from os.path import join, pardir

DATA_DIR = join(pardir, pardir, 'data', 'microbenchmark_primitives')
df = pd.read_csv(join(DATA_DIR, 'result.csv'), index_col=0)
print(df)
df['data'] /= 1000.0

for op in ["timelock_acquire", "timelock_release", "atomic_counter", "atomic_list_increase"]:


    mean = df.loc[df['op'] == op].groupby(['size']).mean()
    std_dev = df.loc[df['op'] == op].groupby(['size']).std()

    grouped = df.loc[df['op'] == op].groupby(['size'])
    for name, group in grouped:
        print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))
    #print(op, 'Median', round(.quantile(0.5)['data'],2))
    #print(op, 'p95', round(df.loc[df['op'] == op].groupby(['size']).quantile(0.95)['data'],2))
    #print(op, 'p99', round(df.loc[df['op'] == op].groupby(['size']).quantile(0.99)['data'],2))
    #print(op, 'min', round(df.loc[df['op'] == op].groupby(['size']).min()['data'],2))
    #print(op, 'max', round(df.loc[df['op'] == op].groupby(['size']).max()['data'],2))
    #print(op, 'cv', std_dev / mean * 100.0)

print("DynamoDB")
DATA_DIR = join(pardir, pardir, 'data', 'microbenchmark_storage')
df = pd.read_csv(join(DATA_DIR, 'intraregion_dynamodb.csv'), index_col=0)
df['data'] /= 1000.0
print(df)
mean = df.loc[df['op'] == 'write'].groupby(['size']).mean()
std_dev = df.loc[df['op'] == 'write'].groupby(['size']).std()
print(std_dev/mean)
grouped = df.loc[df['op'] == 'write'].groupby(['size']) 
for name in [8, 1024, 65536]:
    group = grouped.get_group(name)
    print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))

mean = df.loc[df['op'] == 'read'].groupby(['size']).mean()
std_dev = df.loc[df['op'] == 'read'].groupby(['size']).std()
print(std_dev/mean)
grouped = df.loc[df['op'] == 'read'].groupby(['size']) 
for name in [8, 1024, 65536]:
    group = grouped.get_group(name)
    print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))
