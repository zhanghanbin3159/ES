#-*- coding: UTF-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import os

# file_name = '/opt/ML/data/symbols'
# emailfile = open(file_name, 'r')

def toes(allfiles):
    # 连接到集群，提供节点，不一定要全部节点
    es = Elasticsearch(hosts=[{'host':'192.168.1.156','port':'9200'}],
                       http_auth=('elastic','changeme'))
    print es
    # 打开文件准备读取数据
    for file_name in allfiles:
        if os.path.isdir(file_name):
            continue
        emailfile = open(file_name, 'r')
        actions = []
        for line in emailfile:
            fields = line.split(':')
            if len(fields) != 2:
                continue
            try:
                # print fields
                action = {
                    "_index" : "email_index",
                    "_type" : "email_type",
                    # "_id" : fields[0][0:512],
                    "_source": {
                        "username" : fields[0],
                        "password" : fields[1]
                    }
                }
                actions.append(action)

                if len(actions) == 1000:
                    helpers.bulk(es, actions)
                    actions = []
                    print "insert 1000"
            except Exception as e:
                print e
                pass

        if len(actions) > 0:
            helpers.bulk(es, actions)

        print "insert finish" + file_name


allfile=[]
def getallfile(path):
    allfilelist=os.listdir(path)
    for file in allfilelist:
        filepath=os.path.join(path,file)
        #判断是不是文件夹
        if os.path.isdir(filepath):
            getallfile(filepath)
        allfile.append(filepath)
    return allfile

if __name__ == '__main__':
    #设置编码，避免中文乱码
    reload(sys)
    sys.setdefaultencoding('ISO-8859-9')

    path = "/opt/ML/data/"
    allfiles=getallfile(path)
    toes(allfiles)