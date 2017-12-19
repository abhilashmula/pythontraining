# Start Cloudera Services
#Author Abhilash Mula
#!/bin/python
import time
import sys
import json

from cm_api.endpoints.types import ApiClusterTemplate
from cm_api.endpoints.cms import ClouderaManager
from cm_api.api_client import ApiResource
import cm_api.endpoints.cms
import argparse

cm_host = "192.168.1.122"
cm_port = 7180
cm_username = "admin"
cm_password = "admin"

services = [ 'hue', 'sqoop', 'flume', 'oozie', 'hive', 'hbase', 'mapreduce', 'yarn', 'httpfs', 'hdfs', 'zookeeper', 'spark' ]

#api = ApiResource(cm_host, username="admin", password="admin")
api = ApiResource(cm_host, cm_port, cm_username, cm_password)
cluster_name = "cluster"
cluster = api.get_cluster(cluster_name)
print('cluster...=',cluster)

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='args : start/start, instance-id')
    parser.add_argument('-o', '--op',
                        help='operation type',
                        required='True',
                        default='stop')

    results = parser.parse_args(args)
    return (results.op)

def cdh_services(op):
    for c in api.get_all_clusters():
        print('c.name = ', c.name)
        for s in api.get_cluster(c.name).get_all_services():
            print('s.name =', s.name)
            if s.name in services:
              print('yes')
            else:
              print('no')

        #cluster = api.get_cluster(cluster_name)
        cluster = api.get_cluster(c.name)
        print('cluster2 =',cluster)
        if op == 'stop':
            print('cdh service - stop')
            cluster.stop().wait()
        elif op == 'start':
            print('cdh service - start')
            cluster.start().wait()

def cdh_manager(op):
    cm = ClouderaManager(api)
    cm_service = cm.get_service()
    print('cm_service=',cm_service)
    #restart the management service
    if op == 'stop':
        print('cm service - stop')
        cm_service.stop().wait()
    elif op == 'start':
        print('cm service - restart')
        cm_service.restart().wait()

if __name__ == '__main__':
    op = check_arg(sys.argv[1:])
    cdh_services(op)
    cdh_manager(op)