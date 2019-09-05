#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import os
import sys

import numpy as np
import pymysql

reload(sys)
sys.setdefaultencoding('utf8')

path1 = os.path.abspath('.')  # 表示当前所处的文件夹的绝对路径
path2 = os.path.abspath('..')  # 表示当前所处的文件夹上一级文件夹的绝对路径
path3 = '/Users/amy/Desktop/which projects to participate/code/data/'
path4 = '/Users/amy/Desktop/rls_14/0729/'

conn = pymysql.connect(host='127.0.0.1', port=3306, user='amy', passwd='123456', db='openstack2019', charset='utf8')
cursor = conn.cursor()


nodes = np.loadtxt(path4 + 'rls14_nodes0729.csv', skiprows=1, delimiter=",", dtype=str)
edges = np.loadtxt(path4 + 'rls14_edges0729.csv', skiprows=1, delimiter=",", dtype=str)
dict_edgs = {}
for i in range(len(edges)):
    key = edges[i][0] + ' ' + edges[i][1]
    dict_edgs[key] = int(edges[i][2])
print 'nodes', nodes
print 'dict_edgs', dict_edgs


# 获取每个cluster内部有边相连的节点对以及权重
def inner_cluster(coms, repos, rfrs, clusterID):
    res = []
    coms_cmt = {}
    repos_cmt = {}
    for i in xrange(len(repos)):
        for j in xrange(len(coms)):
            pair1 = repos[i] + ' ' + coms[j]
            print 'pair1', pair1
            if pair1 in rfrs.keys():
                print '*****************************'
                res.append([clusterID, coms[j], repos[i], rfrs[pair1]])

                # 将边信息更新到repo的权重dict
                if repos[i] in repos_cmt.keys():
                    repos_cmt[repos[i]] += rfrs[pair1]
                else:
                    repos_cmt[repos[i]] = rfrs[pair1]

                # 将边信息更新到公司的权重dict
                if coms[j] in coms_cmt.keys():
                    coms_cmt[coms[j]] += rfrs[pair1]
                else:
                    coms_cmt[coms[j]] = rfrs[pair1]

            pair2 = coms[j] + ' ' + repos[i]
            if pair2 in rfrs.keys():
                print '*****************************'
                res.append([clusterID, coms[j], repos[i], rfrs[pair2]])

                # 将边信息更新到repo的权重dict
                if repos[i] in repos_cmt.keys():
                    repos_cmt[repos[i]] += rfrs[pair2]
                else:
                    repos_cmt[repos[i]] = rfrs[pair2]

                # 将边信息更新到公司的权重dict
                if coms[j] in coms_cmt.keys():
                    coms_cmt[coms[j]] += rfrs[pair2]
                else:
                    coms_cmt[coms[j]] = rfrs[pair2]

    return res, coms_cmt, repos_cmt


# 获取每个cluster的代表，及其属性(com or repo)， 即贡献80% commits的公司和接收80% commits的repositories
def obtain_representative(coms_cmt, repos_cmt, sum_cmt, cid):
    pre = []
    coms_cmt = sorted(coms_cmt.items(), key=lambda x: x[1], reverse=True)
    repos_cmt = sorted(repos_cmt.items(), key=lambda x: x[1], reverse=True)
    print 'coms_cmt', coms_cmt
    print 'repos_cmt', repos_cmt
    pre.append([cid, coms_cmt[0][0], 'com'])
    cur_cmts = coms_cmt[0][1]
    for i in xrange(1, len(coms_cmt)):
        cur_cmts += coms_cmt[i][1]
        if cur_cmts/sum_cmt <= 0.8:
            pre.append([cid, coms_cmt[i][0], 'com'])
        else:
            break

    pre.append([cid, repos_cmt[0][0], 'repo'])
    cur_cmts = repos_cmt[0][1]
    for i in xrange(1, len(repos_cmt)):
        cur_cmts += repos_cmt[i][1]
        if cur_cmts / sum_cmt <= 0.8:
            pre.append([cid, repos_cmt[i][0], 'repo'])
        else:
            break
    return pre


# 获取每个cluster的中心公司节点及其commit数
def obtain_center(coms_cmt, cid):
    print 'coms_cmt', coms_cmt
    com_max = max(coms_cmt.items(), key=lambda x: x[1])
    return [cid, com_max[0], com_max[1]]


# 获取每个cluster的按照贡献排名前10的公司、repo列表
def obtain_top_10(coms_cmt, repos_cmt, cid):
    top_10 = []
    coms_cmt = sorted(coms_cmt.items(), key=lambda x: x[1], reverse=True)
    repos_cmt = sorted(repos_cmt.items(), key=lambda x: x[1], reverse=True)
    print 'coms_cmt', coms_cmt
    print 'repos_cmt', repos_cmt
    for i in xrange(0, len(coms_cmt)):
        if i < 10:
            top_10.append([cid, coms_cmt[i][0], 'com'])
        else:
            break

    for j in xrange(0, len(repos_cmt)):
        if j < 10:
            top_10.append([cid, repos_cmt[j][0], 'repo'])
        else:
            break
    return top_10


# 获取每个cluster的节点数，以及比例
details = []  # 存放每个cluster的ID，每条边对应的coms和repos,以及commits数
size = []
contributions = []
centers = []
representatives = []
top_10 = []

cur = 0
CID = 1
cluster_coms = []
cluster_repos = []
for x in xrange(len(nodes)):
    print '************** ', nodes[x][2], ' ************** '
    if int(nodes[x][2]) == CID:
        cur += 1
        if int(nodes[x][1]) == 0:
            cluster_repos.append(nodes[x][0])
        else:
            cluster_coms.append(nodes[x][0])
    else:
        # 保存当前cluster各个维度下的值
        size.append([CID, cur])

        # 获取当前cluster的contribution, center, 以及representatives
        print 'cluster_coms, cluster_repos, dict_edgs, CID', cluster_coms, cluster_repos, CID
        res, coms_cmt, repos_cmt = inner_cluster(cluster_coms, cluster_repos, dict_edgs, CID)
        details.extend(res)
        sum_cmt = sum(coms_cmt.values())
        contributions.append([CID, sum_cmt])
        print 'res, coms_cmt, repos_cmt', res, coms_cmt, repos_cmt
        center = obtain_center(coms_cmt, CID)
        centers.append(center)
        pre = obtain_representative(coms_cmt, repos_cmt, sum_cmt, CID)
        representatives.extend(pre)
        t10 = obtain_top_10(coms_cmt, repos_cmt, CID)
        top_10.extend(t10)

        print 'size', size
        print 'details', details
        print 'contributions', contributions
        print 'centers', centers
        print 'representatives', representatives
        print 'top_10', top_10

        # 更新标记
        CID = int(nodes[x][2])
        cur = 0
        cluster_coms = []
        cluster_repos = []

        cur += 1
        if int(nodes[x][1]) == 0:
            cluster_repos.append(nodes[x][0])
        else:
            cluster_coms.append(nodes[x][0])


# 处理最后一个cluster
size.append([CID, cur])
# 获取当前cluster的contribution, center, 以及representatives
res, coms_cmt, repos_cmt = inner_cluster(cluster_coms, cluster_repos, dict_edgs, CID)
details.extend(res)
sum_cmt = sum(coms_cmt.values())
contributions.append([CID, sum_cmt])
center = obtain_center(coms_cmt, CID)
centers.append(center)
pre = obtain_representative(coms_cmt, repos_cmt, sum_cmt, CID)
representatives.extend(pre)
t10 = obtain_top_10(coms_cmt, repos_cmt, CID)
top_10.extend(t10)


np.savetxt(path4 + 'cluster_details0729.csv', details, delimiter=',', fmt='%s')
np.savetxt(path4 + 'cluster_size0729.csv', size, delimiter=',', fmt='%s')
np.savetxt(path4 + 'cluster_contributions0729.csv', contributions, delimiter=',', fmt='%s')
np.savetxt(path4 + 'cluster_com_centers0729.csv', centers, delimiter=',', fmt='%s')
np.savetxt(path4 + 'cluster_representatives0729.csv', representatives, delimiter=',', fmt='%s')
np.savetxt(path4 + 'cluster_top0729.csv', top_10, delimiter=',', fmt='%s')




