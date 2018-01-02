#-*- coding: UTF-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import os
import threading

def toes(loops, allfiles):
    print '线程%d处理的文件列表:%s\n'%(loops+1,allfiles)
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

                if len(actions) == 10000:
                    helpers.bulk(es, actions)
                    actions = []
                    print "线程%d insert 10000"%(loops+1)
            except Exception as e:
                print e
                pass

        if len(actions) > 0:
            helpers.bulk(es, actions)

        print "线程%d insert finish %s" %(loops+1,file_name)


allfile=[]
def getallfile(path):
    allfilelist=os.listdir(path)
    for file in allfilelist:
        filepath=os.path.join(path,file)
        #判断是不是文件夹
        if os.path.isdir(filepath):
            getallfile(filepath)
        allfile.append(filepath)


def multithread():
    threads=[]
    threads_num = 4
    print '共有线程数:%d个'%threads_num
    per_thread = len(allfile)/threads_num
    for i in range(threads_num):
        if threads_num-i ==1:
            t = threading.Thread(target=toes,args=(i,allfile[i*per_thread:]))
        else:
            t=threading.Thread(target=toes,args=(i,allfile[i*per_thread:i*per_thread+per_thread]))
        threads.append(t)
    for i in range(threads_num):
        threads[i].start()
    for i in range(threads_num):
        threads[i].join()
    print "all insert!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

if __name__ == '__main__':
    #设置编码，都是拉丁文
    reload(sys)
    sys.setdefaultencoding('ISO-8859-9')

    # 连接到集群，提供节点，不一定要全部节点
    es = Elasticsearch(hosts=[{'host':'192.168.1.156','port':'9200'}],
                       http_auth=('elastic','changeme'))
    print es

    path = "/opt/ML/data/"
    getallfile(path)
    print "获取%d个文件："%len(allfile)
    multithread()