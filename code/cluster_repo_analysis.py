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

details = np.loadtxt(path4 + 'cluster_details0729.csv', delimiter=",", dtype=str)

Centrality = ['aodh', 'barbican', 'ceilometer', 'cinder', 'cloudkitty', 'congress', 'designate', 'freezer', 'glance',
              'heat', 'horizon', 'ironic', 'keystone', 'magnum', 'manila', 'mistral', 'monasca-api', 'monasca-log-api',
              'murano', 'neutron', 'nova', 'panko', 'sahara', 'searchlight', 'senlin', 'solum', 'swift', 'tacker',
              'trove', 'vitrage', 'watcher', 'zaqar']
print len(Centrality)

# 获取第14个版本每个repo所属的项目信息（name, description, type, category）
with conn.cursor() as cursor:
    sql = 'SELECT distinct commits_new.repository, project_list.* ' \
          'FROM commits_new, project_list ' \
          'where company is not null ' \
          'and version like %s ' \
          'and commits_new.prj_ID = project_list.id ' \
          'and prj_ID is not null'
    cursor.execute(sql, 14)
    repos_details = cursor.fetchall()
print 'repos_details', repos_details


# 获取repository - project(name + description), repo -  type
repo_prj = {}
repo_type = {}
for i in xrange(len(repos_details)):
    key = repos_details[i][0]
    value1 = repos_details[i][2] + ' -- ' + (repos_details[i][3].replace('\n', ' ')).replace('\r', ' ')
    value2 = repos_details[i][4]
    repo_prj[key] = value1
    repo_type[key] = value2

print 'repo_prj', repo_prj
print 'repo_type', repo_type


res = []
CID = 1

ct = 0
ct_list = []
dist_repo = {}
dict_prj = {}
dict_type = {}
for i in xrange(len(details)):
    if int(details[i][0]) == CID:
        repo = details[i][2]
        if repo in Centrality and repo not in ct_list:
            ct += 1
            ct_list.append(repo)
        if repo in dist_repo.keys():
            dist_repo[repo] += int(details[i][3])
        else:
            dist_repo[repo] = int(details[i][3])

        if repo in repo_prj.keys():
            prj = repo_prj[repo]
            if prj in dict_prj.keys():
                dict_prj[prj] += int(details[i][3])
            else:
                dict_prj[prj] = int(details[i][3])
        if repo in repo_type.keys():
            type = repo_type[repo]
            if type in dict_type.keys():
                dict_type[type] += int(details[i][3])
            else:
                dict_type[type] = int(details[i][3])
    else:
        dist_repo = sorted(dist_repo.items(), key=lambda x: x[1], reverse=True)
        dict_prj = sorted(dict_prj.items(), key=lambda x: x[1], reverse=True)
        dict_type = sorted(dict_type.items(), key=lambda x: x[1], reverse=True)
        res.append([CID, ct, ' '.join(ct_list), dist_repo[0][0], dict_prj[0][0], dict_type[0][0]])

        CID = int(details[i][0])
        ct = 0
        ct_list = []
        dist_repo = {}
        dict_prj = {}
        dict_type = {}

        if int(details[i][0]) == CID:
            repo = details[i][2]
            if repo in Centrality and repo not in ct_list:
                ct += 1
                ct_list.append(repo)
            if repo in dist_repo.keys():
                dist_repo[repo] += int(details[i][3])
            else:
                dist_repo[repo] = int(details[i][3])

            if repo in repo_prj.keys():
                prj = repo_prj[repo]
                if prj in dict_prj.keys():
                    dict_prj[prj] += int(details[i][3])
                else:
                    dict_prj[prj] = int(details[i][3])
            if repo in repo_type.keys():
                type = repo_type[repo]
                if type in dict_type.keys():
                    dict_type[type] += int(details[i][3])
                else:
                    dict_type[type] = int(details[i][3])

dist_repo = sorted(dist_repo.items(), key=lambda x: x[1], reverse=True)
dict_prj = sorted(dict_prj.items(), key=lambda x: x[1], reverse=True)
dict_type = sorted(dict_type.items(), key=lambda x: x[1], reverse=True)
res.append([CID, ct, ' '.join(ct_list), dist_repo[0][0], dict_prj[0][0], dict_type[0][0]])

np.savetxt(path4 + 'repo_func_cen0729.csv', res, delimiter=',', fmt='%s')
