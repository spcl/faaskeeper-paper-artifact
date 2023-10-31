#!/usr/bin/env python

import os
import sys

import pandas as pd

from kazoo.client import KazooClient

results = []

def get_children(prefix, children):

    for child in children:

        path = os.path.join(prefix, child)
        if "/zookeeper" in path:
            continue

        children, data = zk.get_children(path, include_data=True)
        results.append([path, data.dataLength])
        get_children(path, children)

server = sys.argv[2]

zk = KazooClient(hosts=f'{server}:2181')
zk.start()

nodes = []

children, data = zk.get_children("/", include_data=True)
get_children("/", children)
df = pd.DataFrame(data=results, columns=['path', 'length'])

zk.stop()

df.to_csv(sys.argv[1])
