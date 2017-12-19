# File: StartHDP26 Services
# Author Abhilash Mula
# !/bin/python
import time
import sys
import json
import requests
from time import sleep
import os

from sys import version_info

AMBARI_DOMAIN = respone = input("Enter Ambari Hostname with FQDN: ")
AMBARI_PORT = respone = input("Enter Ambari port number: ")
AMBARI_USER_ID = respone = input("Enter Ambari user ")
AMBARI_USER_PW = respone = input("Enter Ambari password ")
AMBARI_PROTOCOL = respone = input("Is Ambari HTTP or HTTPS? :")
restAPI = '/api/v1/clusters'
headers = {'X-Requested-By': 'ambari', }

# AMBARI_DOMAIN = "hdp26.informatica.com"
# AMBARI_PORT = "8080"
# AMBARI_USER_ID = "admin"
# AMBARI_USER_PW = "admin"
# AMBARI_PROTOCOL = "http"


if AMBARI_PROTOCOL == "http":
    url = AMBARI_PROTOCOL + "://" + AMBARI_DOMAIN + ":" + AMBARI_PORT + restAPI
    print("Ambari REST URL:", url)

if AMBARI_PROTOCOL == "https":
    url = AMBARI_PROTOCOL + "://" + AMBARI_DOMAIN + ":" + AMBARI_PORT + restAPI
    print("Ambari REST URL:", url)

# Get CLuster Name
r = requests.get(url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
json_data = json.loads(r.text)
CLUSTER_NAME = json_data["items"][0]["Clusters"]["cluster_name"]
print("Name of the Cluster is :", CLUSTER_NAME)

# Service url's
zk_url = url + "/" + CLUSTER_NAME + "/services/ZOOKEEPER"
hbase_url = url + "/" + CLUSTER_NAME + "/services/HBASE"
hdfs_url = url + "/" + CLUSTER_NAME + "/services/HDFS"
yarn_url = url + "/" + CLUSTER_NAME + "/services/YARN"
mr2_url = url + "/" + CLUSTER_NAME + "/services/MAPREDUCE2"
hive_url = url + "/" + CLUSTER_NAME + "/services/HIVE"
spark2_url = url + "/" + CLUSTER_NAME + "/services/SPARK2"

# Get Cluster Version

CLUSTER_VERSION = json_data["items"][0]["Clusters"]["version"]
print("Cluster Version is :", CLUSTER_VERSION, '\n' * 2)


# Check zookeeper service
def zk_service_check(zk_service_status):
    zk_temp = requests.get(zk_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    zk_service_json_data = json.loads(zk_temp.text)
    return zk_service_status == zk_service_json_data["ServiceInfo"]["state"]
    print("zookeeper service status is ", zookeeper_service_status, '\n' * 1)


if zk_service_check(zk_service_status="STARTED"):
    print("Zookeeper service is already started", '\n')

if zk_service_check(zk_service_status="INSTALLED"):
    print("Zookeeper service is stopped", '\n')
    data = '{"RequestInfo": {"context" :"Start ZOOKEEPER via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(zk_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("Zookeeper service starting in 60 secs.....", '\n')
    sleep(60)  # Time in seconds.
    if zk_service_check(zk_service_status="STARTED"):
        print("Zookeeper service is started", '\n')
    else:
        print("Zookeeper service did not start,please check logs at /var/log/zookeeper", '\n')


# Check HDFS service
def hdfs_service_check(hdfs_service_status):
    hdfs_temp = requests.get(hdfs_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    hdfs_service_json_data = json.loads(hdfs_temp.text)
    return hdfs_service_status == hdfs_service_json_data["ServiceInfo"]["state"]
    print("HDFS service status is ", hdfs_service_status)


if hdfs_service_check(hdfs_service_status="STARTED"):
    print("HDFS service is already started", '\n')

if hdfs_service_check(hdfs_service_status="INSTALLED"):
    print("HDFS service is stopped", '\n')
    data = '{"RequestInfo": {"context" :"Start HDFS via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(hdfs_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("HDFS service is starting.....", '\n')
    sleep(90)  # Time in seconds.
    if hdfs_service_check(hdfs_service_status="INSTALLED"):
        os.system('hdfs dfsadmin -safemode leave')
    sleep(30)
    if hdfs_service_check(hdfs_service_status="STARTED"):
        print("HDFS service is started", '\n')
    else:
        print("HDFS service did not start,please check logs at /var/log/hdfs", '\n')


# Check Yarn service
def yarn_service_check(yarn_service_status):
    yarn_temp = requests.get(yarn_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    yarn_service_json_data = json.loads(yarn_temp.text)
    return yarn_service_status == yarn_service_json_data["ServiceInfo"]["state"]
    print("Hbase service status is ", hbase_service_status)


if yarn_service_check(yarn_service_status="STARTED"):
    print("Yarn service is already started", '\n')

if yarn_service_check(yarn_service_status="INSTALLED"):
    print("Yarn service is stopped", '\n')
    data = '{"RequestInfo": {"context" :"Start YARN via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(yarn_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("Yarn service is starting.....", '\n')
    sleep(180)  # Time in seconds.
    if yarn_service_check(yarn_service_status="STARTED"):
        print("YARN service is started", '\n')
    else:
        print("Yarn service did not start,please check logs at /var/log/yarn", '\n')


# Check MR2 service

def mr2_service_check(mr2_service_status):
    mr2_temp = requests.get(mr2_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    mr2_service_json_data = json.loads(mr2_temp.text)
    return mr2_service_status == mr2_service_json_data["ServiceInfo"]["state"]
    print("MR2 service status is ", MR2_service_status)


if mr2_service_check(mr2_service_status="STARTED"):
    print("MR2 service is already started", '\n')

if mr2_service_check(mr2_service_status="INSTALLED"):
    print("MR2 service is stopped", '\n')
    data = '{"RequestInfo": {"context" :"Start MR2 via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(mr2_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("MR2 service is starting.....", '\n')
    sleep(60)  # Time in seconds.
    if mr2_service_check(mr2_service_status="STARTED"):
        print("MR2 service is started", '\n')
    else:
        print("MR2 service did not start,please check logs at /var/log/hbase", '\n')


# Check Hbase service
def hbase_service_check(hbase_service_status):
    hbase_temp = requests.get(hbase_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    hbase_service_json_data = json.loads(hbase_temp.text)
    return hbase_service_status == hbase_service_json_data["ServiceInfo"]["state"]
    print("Hbase service status is ", hbase_service_status)


if hbase_service_check(hbase_service_status="STARTED"):
    print("HBASE service is already started", '\n')

if hbase_service_check(hbase_service_status="INSTALLED"):
    print("Hbase service is stopped", '\n')
    print("Starting Hbase Service")
    data = '{"RequestInfo": {"context" :"Start HBASE via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(hbase_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("Hbase service starting in 60 secs.....", '\n')
    sleep(60)  # Time in seconds.
    if hbase_service_check(hbase_service_status="STARTED"):
        print("Hbase service is started", '\n')
    else:
        print("Hbase service did not start,please check logs at /var/log/hbase", '\n')


# Check Hive service
def hive_service_check(hive_service_status):
    hive_temp = requests.get(hive_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    hive_service_json_data = json.loads(hive_temp.text)
    return hive_service_status == hive_service_json_data["ServiceInfo"]["state"]
    print("Hive service status is ", hive_service_status)


if hive_service_check(hive_service_status="STARTED"):
    print("HIVE service is already started", '\n')

if hive_service_check(hive_service_status="INSTALLED"):
    print("HIVE service is stopped", '\n')
    print("Starting HIVE Service")
    data = '{"RequestInfo": {"context" :"Start HIVE via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(hive_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("Hive service is starting.....", '\n')
    sleep(60)  # Time in seconds.
    if hive_service_check(hive_service_status="STARTED"):
        print("Hive service is started", '\n')
    else:
        print("Hive service did not start,please check logs at /var/log/hive", '\n')


# Check Spark2 service
def spark2_service_check(spark2_service_status):
    spark2_temp = requests.get(spark2_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    spark2_service_json_data = json.loads(spark2_temp.text)
    return spark2_service_status == spark2_service_json_data["ServiceInfo"]["state"]
    print("Soark2 service status is ", spark2_service_status)


if spark2_service_check(spark2_service_status="STARTED"):
    print("Spark2 service is already started", '\n')

if spark2_service_check(spark2_service_status="INSTALLED"):
    print("spark2 service is stopped", '\n')
    print("Starting spark2 Service")
    data = '{"RequestInfo": {"context" :"Start Spark2 via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(spark2_url, headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("spark2 service is starting.....", '\n')
    sleep(30)  # Time in seconds.
    if spark2_service_check(spark2_service_status="STARTED"):
        print("Spark2 service is started", '\n')
    else:
        print("Spark2 service did not start,please check logs at /var/log/spark2", '\n')
