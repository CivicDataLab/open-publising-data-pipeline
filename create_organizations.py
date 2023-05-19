'Class to create CKAN organizations'

import argparse
import json
import re
import os
import requests

class CreateOrganizations(object):
    def read_file_for_titles(self, input_file_path):
        titles = []
        with open(input_file_path) as file_obj:
            titles = file_obj.readlines()
        return titles

    def get_org_handle(self, title):
        handle = re.sub('[^\w]', ' ', title)
        handle = re.sub('Statement showing ', '', handle.strip())
        #handle = re.sub('and', '-', handle.strip())
        handle = re.sub('\s{2,}', ' ', handle)
        handle = re.sub('\s', '-', handle.strip())
        handle = handle[:100]
        #handle = handle[:80]
        # print(handle)
        return handle.lower()

    def get_tag_list(self, tag_string):
        tag_list = []
        tag_string = list(eval(tag_string))
        for tag in tag_string:
            tag = tag.split("(")[0].strip()
            tag = tag.replace("&", "and")
            tag = re.sub('[^0-9a-zA-Z\s]', ' ', tag)
            tag = re.sub('\s{2,}', '', tag)
            if tag:
                tag_map = {"name": tag.lower()}
                tag_list.append(tag_map)
        return tag_list


    def create_organizations_from_file(self, base_url, auth_key, input_file_path):
        titles = self.read_file_for_titles(input_file_path)
        for title in titles:
            self.create_organization(base_url, auth_key, title, "expenditure-union-budget")

    def create_organization(self, base_url, auth_key, title, parent, notes="", keywords="", unit="", source=""):
            payload = {}
            org_handle = self.get_org_handle(title)
            #org_handle = org_handle.replace("-budget", "")
            payload["title"] = title.strip()
            payload["name"] = org_handle
            payload["id"] = payload["name"]
            if notes:
                payload["description"] = notes
            if keywords:
                payload["tags"] = self.get_tag_list('[%s]' % keywords)
            payload["groups"] = [{}]
            payload["groups"][0]["capacity"] = "public"
            payload["groups"][0]["name"] = parent
            payload["extras"] =[{"key":"Keywords", "value":keywords}, {"key":"Source", "value":source}, {"key":"Unit", "value":unit}]
            #print(payload)
            payload_json = json.dumps(payload)
            resource_url = "%s/api/action/organization_create" % base_url
            auth_dict = {"X-CKAN-API-Key" : auth_key, 'content-type': 'application/json'}
            try:
                response = requests.post(resource_url, headers=auth_dict, data=payload_json, verify=False)
                if response.status_code != 200:
                   print(payload_json)
                   raise Exception('Payload error: %s' % response.text)
            except Exception as error_message:
                print("Unable to upload for department: %s, error_message: %s" % (title, error_message))
            #print(command)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create CKAN Portal Organizations")
    parser.add_argument("base_url", help="Base URL of the portal")
    parser.add_argument("auth_key", help="Authorization key to the portal")
    parser.add_argument("input_file_path", help="Input text filepath containing name of organizations")
    args = parser.parse_args()
    obj = CreateOrganizations()
    if not args.input_file_path or not args.base_url or not args.auth_key:
        print("Please pass required arguments: base_url, auth_key and input_pdf_filepath")
    else:
        obj.create_organizations_from_file(args.base_url, args.auth_key, args.input_file_path)