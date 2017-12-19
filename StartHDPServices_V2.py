import time
import sys
import json
import requests
from time import sleep
import os

from sys import version_info

restAPI = '/api/v1/clusters'
headers = {'X-Requested-By': 'ambari', }
AMBARI_DOMAIN = "hdp26.informatica.com"
AMBARI_PORT = "8080"
AMBARI_USER_ID = "admin"
AMBARI_USER_PW = "admin"
AMBARI_PROTOCOL = "http"
all_services_list = []
restAPI = '/api/v1/clusters'
headers = {'X-Requested-By': 'ambari', }

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
service_url = url + "/" + CLUSTER_NAME + "/services/"

# List All Services
all_services_url = url + "/" + CLUSTER_NAME + "/services"
all_services_post = requests.get(all_services_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW)).json()
# all_services_json_data = json.loads(all_services_post)
for items in all_services_post['items']:
    all_services_temp = items['ServiceInfo']
    all_services = all_services_temp['service_name']
    # print('Service Name:', all_services)
    all_services_list.append(all_services)

# get status of all services
print('\n', "Getting status of all services in Ambari", '\n')

for num, service_name in enumerate(all_services_list):
    service_temp = requests.get(service_url + service_name, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    service_json_data = json.loads(service_temp.text)
    service_status = service_json_data["ServiceInfo"]["state"]
    print(service_name, "service status is ", service_status)


def individual_service_status(input_service_name):
    if input_service_name in all_services_list:
        input_service_name_temp = requests.get(service_url + input_service_name, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
        input_service_json_data = json.loads(input_service_name_temp.text)
        return input_service_json_data["ServiceInfo"]["state"]


def start_individual_service(start_service_name):
    if individual_service_status(input_service_name=start_service_name):
        start_service_url = service_url + start_service_name
        data = '{"RequestInfo": {"context" :"Start' + start_service_name + 'via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        response = requests.put(start_service_url, headers=headers, data=data,
                                auth=(AMBARI_USER_ID, AMBARI_USER_PW))
        curr_service_status_url = json.loads(response.content)['href']
        data_curr_service_status = requests.get(curr_service_status_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
        curr_service_status_json_data = json.loads(data_curr_service_status.text)
        print(curr_service_status_json_data)
        curr_service_status = curr_service_status_json_data["Requests"]["request_status"]
        return curr_service_status

start_individual_service("HBASE")








