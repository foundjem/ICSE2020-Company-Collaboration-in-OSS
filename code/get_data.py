#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import pymysql
import networkx as nx
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf8')


conn = pymysql.connect(host='127.0.0.1', port=3306, user='amy', passwd='123456', db='openstack2019', charset='utf8')
cursor = conn.cursor()

path = '/Users/amy/Desktop/which projects to participate/code/data/model/'


# 获取每个版本的公司贡献概况（#dvpr, #cmt）
def search_com(rls):
    with conn.cursor() as cursor:
        sql = 'SELECT com_ID, company, count(distinct author_ID), count(distinct id), count(distinct repository) ' \
              'FROM openstack2019.commits_new ' \
              'where company is not null ' \
              'and version like %s ' \
              'group by com_ID, company '
        cursor.execute(sql, rls)
        res = cursor.fetchall()
    return res


# 获取当前版本有活动的公司列表和项目列表，以及不同公司对不同项目的贡献情况
def search_com_repo(rls):
    with conn.cursor() as cursor:
        sql = 'SELECT company, repository, count(distinct id) ' \
              'FROM openstack2019.commits_new ' \
              'where company is not null ' \
              'and version like %s ' \
              'group by company, repository'
        cursor.execute(sql, rls)
        coms_repos = cursor.fetchall()

        sql = 'SELECT distinct company ' \
              'FROM openstack2019.commits_new ' \
              'where company is not null ' \
              'and version like %s'
        cursor.execute(sql, rls)
        coms = cursor.fetchall()
        res_coms =[]
        for i in xrange(len(coms)):
            res_coms.append(coms[i][0])

        sql = 'SELECT distinct repository ' \
              'FROM openstack2019.commits_new ' \
              'where company is not null ' \
              'and version like %s'
        cursor.execute(sql, rls)
        repos = cursor.fetchall()
        res_repos = []
        for i in xrange(len(repos)):
            res_repos.append(repos[i][0])

    return res_coms, res_repos, coms_repos


# 构建当前版本的公司协作网络
def build_com_network(rls, coms, repos, coms_repos):
    num_com = len(coms)
    num_repo = len(repos)
    CR = np.zeros((num_com, num_repo), dtype=np.int16)

    # 向公司项目矩阵填值
    for i in xrange(len(coms_repos)):
        print 'coms', coms
        print 'coms_repos[i][0]', coms_repos[i][0]
        row = coms.index(coms_repos[i][0])
        col = repos.index(coms_repos[i][1])
        CR[row][col] = coms_repos[i][2]
    print 'com_repo', CR

    # 计算两两公司之间的共同项目数, 构建网络输入数据
    G = nx.Graph()
    for i in xrange(num_com-1):
        for j in xrange(i + 1, num_com):
            for r in xrange(num_repo):
                if CR[i][r] != 0 and CR[j][r] != 0:
                    if coms[i] not in G:
                        G.add_node(coms[i])
                    if coms[j] not in G:
                        G.add_node(coms[j])
                    G.add_edge(coms[i], coms[j])
                    break
    nx.write_gexf(G, path + '/coms' + str(rls) + '.gexf')
    return G

records_full = []
records = []
for i in xrange(1, 19):
    print '*********************** ' + str(i) + ' ***********************'
    com_info = search_com(i)
    coms, repos, coms_repos = search_com_repo(i)
    G = build_com_network(i, coms, repos, coms_repos)

    DC = nx.degree_centrality(G)
    BC = nx.betweenness_centrality(G)
    CC = nx.closeness_centrality(G)
    EC = nx.eigenvector_centrality(G)

    for j in xrange(len(com_info)):
        if com_info[j][1] in G.nodes():
            dc = DC[com_info[j][1]]
            bc = BC[com_info[j][1]]
            cc = CC[com_info[j][1]]
            ec = EC[com_info[j][1]]
            records.append([i, com_info[j][0], com_info[j][1], com_info[j][2], com_info[j][3], com_info[j][4], dc, bc,
                            cc, ec])
            records_full.append([i, com_info[j][0], com_info[j][1], com_info[j][2], com_info[j][3], com_info[j][4], dc,
                                 bc, cc, ec])

        else:
            records_full.append([i, com_info[j][0], com_info[j][1], com_info[j][2], com_info[j][3], com_info[j][4], 0,
                                 0, 0, 0])
    print 'records', records
np.savetxt(path + "model_data.csv", records, delimiter=',', fmt='%s')
np.savetxt(path + "model_data_full.csv", records_full, delimiter=',', fmt='%s')


conn.commit()
cursor.close()
conn.close()

