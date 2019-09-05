#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import sys
import networkx as nx
import numpy as np

reload(sys)
sys.setdefaultencoding('utf8')

path4 = '/Users/amy/Desktop/rls_14/0729/'


nodes = np.loadtxt(path4 + 'rls14_nodes0729.csv', skiprows=1, delimiter=",", dtype=str)
edges = np.loadtxt(path4 + 'rls14_edges0729.csv', skiprows=1, delimiter=",", dtype=str)
print 'nodes', nodes
print 'edges', edges

G = nx.Graph()
# 生成gexf格式的文件 节点（com, repo), 边表示提交过commits, 权重为commits个数
G = nx.Graph()
for j in xrange(len(nodes)):
    G.add_node(nodes[j][0])
    G.nodes[nodes[j][0]]['group'] = int(nodes[j][1])
    G.nodes[nodes[j][0]]['cluster_ID'] = int(nodes[j][2])
print 'j', j
for i in xrange(len(edges)):
    if edges[i][0] in G and edges[i][1] in G:
        G.add_edge(edges[i][0], edges[i][1], weight= int(edges[i][2]))
print 'i', i

nx.write_gexf(G, path4 + 'coms_repos14.gexf')

