#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
from os.path import join, pardir
DATA_DIR = join(pardir, pardir, 'data', 'write_new')
DATA_DIR_ZK = join(pardir, pardir, 'data', 'write_zookeeper')

dfs = []
for mem in [512, 1024, 2048]:
    
    for size in [4, 1024, 65536, 131072, 256000]:
        df = pd.read_csv(join(DATA_DIR, f'persistent_{size}_{mem}_processed.csv'), index_col=0)
        df['size'] = size / 1024
        
        #print(df)
        # data is broken :-(
        #df = df.loc[df['memory'] == mem]
        #df = df.groupby(['memory', 'function']).head(150).reset_index(drop=True)
        #if df.shape[0] > 200:
        #    print(size, mem)
        dfs.append(df)
        
        
dynamo_df = pd.concat(dfs)
dynamo_df['system'] = 'persistent'
print(dynamo_df)

dynamo_df['cost'] = dynamo_df['billed_time'] * dynamo_df['memory']
print(dynamo_df.groupby(['function', 'memory', 'size']).median()['cost'])
print(dynamo_df.groupby(['function', 'memory', 'size']).median()['write'])
print(dynamo_df.groupby(['function', 'memory', 'size']).mean()['write'])

#import pandas as pd
#import numpy as np
#from os.path import join, pardir
#
#DATA_DIR = join(pardir, pardir, 'data', 'microbenchmark_primitives')
#df = pd.read_csv(join(DATA_DIR, 'result.csv'), index_col=0)
#print(df)
#df['data'] /= 1000.0
#
#for op in ["timelock_acquire", "timelock_release", "atomic_counter", "atomic_list_increase"]:
#
#
#    mean = df.loc[df['op'] == op].groupby(['size']).mean()
#    std_dev = df.loc[df['op'] == op].groupby(['size']).std()
#
#    grouped = df.loc[df['op'] == op].groupby(['size'])
#    for name, group in grouped:
#        print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))
#    #print(op, 'Median', round(.quantile(0.5)['data'],2))
#    #print(op, 'p95', round(df.loc[df['op'] == op].groupby(['size']).quantile(0.95)['data'],2))
#    #print(op, 'p99', round(df.loc[df['op'] == op].groupby(['size']).quantile(0.99)['data'],2))
#    #print(op, 'min', round(df.loc[df['op'] == op].groupby(['size']).min()['data'],2))
#    #print(op, 'max', round(df.loc[df['op'] == op].groupby(['size']).max()['data'],2))
#    #print(op, 'cv', std_dev / mean * 100.0)
#
#print("DynamoDB")
#DATA_DIR = join(pardir, pardir, 'data', 'microbenchmark_storage')
#df = pd.read_csv(join(DATA_DIR, 'intraregion_dynamodb.csv'), index_col=0)
#df['data'] /= 1000.0
#print(df)
#mean = df.loc[df['op'] == 'write'].groupby(['size']).mean()
#std_dev = df.loc[df['op'] == 'write'].groupby(['size']).std()
#print(std_dev/mean)
#grouped = df.loc[df['op'] == 'write'].groupby(['size']) 
#for name in [8, 1024, 65536]:
#    group = grouped.get_group(name)
#    print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))
#
#mean = df.loc[df['op'] == 'read'].groupby(['size']).mean()
#std_dev = df.loc[df['op'] == 'read'].groupby(['size']).std()
#print(std_dev/mean)
#grouped = df.loc[df['op'] == 'read'].groupby(['size']) 
#for name in [8, 1024, 65536]:
#    group = grouped.get_group(name)
#    print(name, " & ", round(group.min()['data'], 2), " & ", round(group.quantile(0.5)['data'], 2), " & ", round(group.quantile(0.95)['data'], 2), " & ", round(group.quantile(0.99)['data'], 2), " & ", round(group.max()['data'],2))
