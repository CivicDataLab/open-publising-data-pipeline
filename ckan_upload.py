'Class to create CKAN datasets'

import argparse
import csv
import glob
from create_organizations import CreateOrganizations
import json
import os
import re
import requests
import time
import subprocess
#
# TEMP_INDEX_FILE = "/tmp/page.html"
# TEMP_HTML_FILE = "/tmp/pages.html"
# LOG_FILE = "/tmp/log"
# FILE_TYPE_LIST = ["pdf"]


class CreateStatesDatasets(object):
    def __init__(self):
        self.org_creator = CreateOrganizations()

    def get_modified_dataset(self, dataset_name, org_name, year_name):
        if not re.findall(r"^%s-" % org_name, dataset_name):
            dataset_name = org_name + " Budget " + year_name + ": " + dataset_name
        return dataset_name

    def get_modified_handle(self, dataset_handle, state_slug, year_name):
        max_len = 100 - len(year_name)
        if not re.findall(r"^%s-" % state_slug, dataset_handle):
            dataset_handle = "%s-%s" % (state_slug, dataset_handle)
        if not re.findall(r"-%s$" % year_name, dataset_handle):
            dataset_handle = "%s-%s" % (dataset_handle[:max_len - 1], year_name)
        return dataset_handle

    '''
            year_name = os.path.basename(dir_name)
            org_name = "Karnataka Budget " + year_name    
            org_handle = self.org_creator.get_org_handle(org_name) 
            self.org_creator.create_organization(base_url, auth_key, org_name, "karnataka-budget")
    '''

    def create_docs_for_dir(self, base_url, auth_key, input_dir, state_slug):
        for state_dir in glob.glob("%s/*" % input_dir):
            print("a", state_dir)
            state_name = os.path.basename(state_dir)
            state_handle = self.org_creator.get_org_handle(state_name)
            print(state_handle)
            # self.org_creator.create_organization(base_url, auth_key, state_name, "individual-state-budgets")
            for year_dir in glob.glob("%s/*" % state_dir):
                year_name = os.path.basename(year_dir)
                year_org_name = state_name + " Budget " + year_name
                year_org_handle = self.org_creator.get_org_handle(year_org_name)
                print(year_org_handle)
                # self.org_creator.create_organization(base_url, auth_key, year_org_name, state_handle)
                '''
                for folder_dir in glob.glob("%s/*" % year_dir):
                    folder_name = os.path.basename(folder_dir)
                    sub_org_handle = self.org_creator.get_org_handle(folder_name) 
                    print(folder_name)
                    self.org_creator.create_organization(base_url, auth_key, folder_name, year_org_handle)
                '''
                sub_org_handle = year_org_handle
                dataset_list = glob.glob("%s/*" % year_dir)
                dataset_list.sort(reverse=True)
                for dataset_dir in dataset_list:
                    '''
                    sub_org_handle =  year_org_handle
                    dataset_list = glob.glob("%s/*" % year_dir)
                    dataset_list.sort(reverse=True)
                    for dataset_dir in dataset_list:    
                    sub_org_handle =  org_handle
                    resource_name = os.path.basename(dataset_dir)
                    resource_name,file_extension = os.path.splitext(resource_name) 
                    resource_name = self.get_modified_dataset(resource_name, "MP State", year_name)
                    '''
                    dataset_name = os.path.basename(dataset_dir)
                    dataset_name, file_extension = os.path.splitext(dataset_name)
                    dataset_name = re.sub(r"^\d{,2}\s{0,}[\.\-]{,1}\s{0,}", "", dataset_name)
                    if re.findall(r" - ", dataset_dir):
                        dataset_name = dataset_name.split(" - ")[0]
                    dataset_handle = self.org_creator.get_org_handle(dataset_name)
                    dataset_handle = self.get_modified_handle(dataset_handle, state_slug, year_name)
                    print("a", dataset_handle)
                    dataset_name = self.get_modified_dataset(dataset_name, state_name, year_name)
                    resource_name = dataset_name
                    print("r", resource_name)
                    print("b", dataset_name)
                    # package_json = subprocess.check_output("curl https://openbudgetsindia.org/api/action/package_show -H \"X-CKAN-API-Key:a5024363-2339-4f0d-90f1-4d1c4cd4aef5\" -d '{\"id\": \"%s\"}'" % dataset_handle, shell=True)
                    # package_dict = json.loads(package_json)
                    file_type_dict = {"pdf": True}
                    # if 'result' in package_dict and 'resources' in package_dict['result']:
                    # for resource_dict in package_dict['result']['resources']:
                    # if resource_dict['name'] == resource_name:
                    # file_type_dict.pop(resource_dict['format'].lower())
                    # file_type_list = file_type_dict.keys()
                    # else:
                    file_type_list = file_type_dict.keys()
                    payload = {}
                    payload["name"] = dataset_handle
                    payload["title"] = dataset_name
                    payload["owner_org"] = sub_org_handle
                    payload["notes"] = dataset_name
                    package_url = "%s/api/3/action/package_create" % base_url
                    auth_dict = {"Authorization": auth_key, 'content-type': 'application/json'}
                    print(payload)
                    try:
                        response = requests.post(package_url, headers=auth_dict, data=json.dumps(payload))
                        if response.status_code != 200:
                            print('Payload error: %s' % response.text)
                            time.sleep(5)
                    except Exception as e:
                        print("Unable to upload dataset: %s, error_message: %s" % (dataset_handle, e))
                    print(file_type_list)
                    resource_url = "%s/api/action/resource_create" % base_url
                    auth_dict = {"X-CKAN-API-Key": auth_key}
                    upload_list = file_type_dict.keys()
                    # upload_list.sort()
                    for file_type in upload_list:
                        try:
                            resource_dict = {}
                            resource_dict["url"] = ""
                            resource_dict["package_id"] = dataset_handle
                            resource_dict["owner_org"] = sub_org_handle
                            print("d", dataset_dir)
                            file_to_upload = [('upload', open(dataset_dir, 'rb'))]
                            resource_dict["name"] = resource_name
                            resource_dict["description"] = resource_name
                            print(resource_dict)
                            response = requests.post(resource_url, headers=auth_dict, data=resource_dict,
                                                     files=file_to_upload)
                            print(response.text, "%#%#%#%#%#%##%")
                            if response.status_code != 200:
                                print('Payload error: %s' % response.text)
                            time.sleep(10)
                        except Exception as e:
                            print("Unable to upload resource: %s, error_message: %s" % (dataset_name, e))


# if __name__ == '__main__':
#     # parser = argparse.ArgumentParser(description="Create CKAN Portal Datasets")
#     # parser.add_argument("base_url", help="Base URL of the portal")
#     # parser.add_argument("auth_key", help="Authorization key to the portal")
#     # parser.add_argument("input_dir", help="Input directory path for documents")
#     # parser.add_argument("state_slug", help="Input state slug")
#     # args = parser.parse_args()
#     obj = CreateStatesDatasets()
#     # if not args.input_dir:
#     # print("Please input directory for document creation")
#     # else:
#     obj.create_docs_for_dir("https://openbudgetsindia.org/", "7e412837-f3ee-4f9d-b8ee-31f066acde5d",
#                             "/home/abhinav/PF/state-budget-upload", "sikkim")