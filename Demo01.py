l = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(l)
for num in l:
    if num % 2 == 0:
        print(str(num) + ' is an even number')
    else:
        print(str(num) + ' is an odd number')

list_sum = 0
for num in l:
    list_sum = list_sum + num
    print(list_sum)




if 'ZOOKEEPER' in all_services_list:
    zookeeper_service_url = service_url+'ZOOKEEPER'


if 'HBASE' in all_services_list and individual_service_status(input_service_name='HBASE',return_service_status='INSTALLED'):
    data = '{"RequestInfo": {"context" :"Start Hbase via Python"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
    response = requests.put(service_url+'HBASE', headers=headers, data=data,
                            auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    print(json.loads(response.content))
    if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
        print("Hbase service starting.....", '\n')


for index in range(len(all_services_list)):
    while index+1==len(all_services_list):
        print(all_services_list[index])for index in range(len(all_services_list)):
    while index+1==len(all_services_list):
        print(all_services_list[index])

        count = 0
        while (count < len(all_services_list)):
            print('the service name is :', all_services_list[count])
         if all_services_list[count] == 'HBASE':






if (json.loads(response.content)["Requests"]["status"]) == "Accepted":
                 curr_service_status_url = json.loads(response.content)['href']
                 data_curr_service_status = requests.get(curr_service_status_url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
                 curr_service_status_json_data = json.loads(data_curr_service_status.text)
                 print(curr_service_status_json_data)
                 curr_service_status = curr_service_status_json_data["Requests"]["request_status"]
                 print(curr_service_status)
                 while curr_service_status != 'STARTED':
                     print("Status of ", start_service_name, "is", curr_service_status)