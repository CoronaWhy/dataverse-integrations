import sys
import os
import re
import base64
import requests
import hashlib 
import time
import urllib
import json

import subprocess as sp
import pandas as pd

from datetime import datetime
from github import Github
from pyDataverse.api import Api,NativeApi
from pyDataverse.models import Datafile, Dataset

from config import DV_ALIAS, BASE_URL, API_TOKEN, REPO, GITHUB_TOKEN, PARSABLE_EXTENSIONS, gitroot, gituserroot, gitblob, gitmaster
#REPO='J535D165/CoronaWatchNL'
REPO='Mike-Honey/covid-19-vic-au'

class DataverseData():
    def __init__(self, REPO, validate=False):
        self.ext=PARSABLE_EXTENSIONS
        self.REPO=REPO
        self.mapping_dsid2pid = {}
        self.validate_df = validate
        self.g = Github(GITHUB_TOKEN)
        self.repo = self.g.get_repo(REPO)
        self.urls_found = {}
        self.ds_id = 0
        self.DEBUG = True
        
    def datasync(self):
        native_api = NativeApi(BASE_URL, API_TOKEN)
        self.ds_id = str(int(self.make_dataset_id(self.REPO).hexdigest(), 16))[:6] ## turn the md5 string into a 6 digits integer
        metadata = self.make_dataset_metadata(self.REPO)
        self.ds = Dataset()
        self.ds.set(metadata)
        self.ds.displayName=metadata['title']
        if self.DEBUG:
            print(self.ds_id)
            print(self.ds.displayName)
        self.create_dataset(native_api, self.ds, DV_ALIAS, self.ds_id, BASE_URL)
        if self.DEBUG:
            print(metadata)
        self.upload_files_to_dataverse(self.ds_id, self.urls_found)
        return True

    def extract_urls(self, content: str)->list:
        matches = re.findall(r"(http[^\s'\"\\]+)", content)
        pattern = re.compile(r"([^/\w]+)$")
        return [pattern.sub("", match) for match in matches]
    
    def decode_github_content(self, content: str) -> str:
        return base64.b64decode(content).decode("utf-8")
    
    def make_dataset_id(self, repo_name):
        return hashlib.md5(repo_name.encode("utf-8"))
    
    def make_default_dataset(self, data, repo_name):
        ds_id = self.make_dataset_id(repo_name)    
        data[ds_id] = {'metadata': make_dataset_metadata(repo_name)}
        return data
    
    def make_dataset_metadata(self, repo_name):
        metadata = {}
        repo = self.g.get_repo(repo_name)
        metadata['termsOfAccess'] = ''
        metadata['title'] = format(repo.description)
        metadata['subtitle'] = 'Automatic uploads from {} github repository'.format(repo_name)
        metadata['author'] = [{"authorName": repo_name,"authorAffiliation": "CoronaWhy"}]
        metadata['dsDescription'] = [{'dsDescriptionValue': ''}]
        metadata['dsDescription'] = [{'dsDescriptionValue': format(repo.get_topics())}]
        if len(metadata['dsDescription']) < 3:
            metadata['dsDescription'] = [ { 'dsDescriptionValue': 'coronavirus' }]

        metadata['subject'] = ['Medicine, Health and Life Sciences']
        #metadata['keyword'] = repo.get_topics()
        metadata['datasetContact'] = [{'datasetContactName': 'https://github.com/{}'.format(repo_name),'datasetContactEmail': 'contact@coronawhy.org'}]
    
        return metadata
    
    def make_file_metadata(self, repo_name, file, url):
        metadata = {}

        metadata['description'] = file
        metadata['filename'] = url
        metadata['datafile_id'] = hashlib.md5(url.encode("utf-8"))
        metadata['dataset_id'] = hashlib.md5(repo_name.encode("utf-8"))
        return metadata
    
    def create_dataset(self, api, ds, dv_alias, ds_id, base_url):
        try:
            resp = api.create_dataset(dv_alias, ds.json())
            pid = resp.json()['data']['persistentId']
        except:
            print(resp.content)
            return resp, self.mapping_dsid2pid
    
        self.mapping_dsid2pid[ds_id] = pid
        time.sleep(1)
        print('{0}/dataset.xhtml?persistentId={1}&version=DRAFT'.format(base_url,
                                                                    pid))
        return resp
    
    # Implementation adapted from http://guides.dataverse.org/en/latest/api/native-api.html#id62
    def upload_datafile(self, server, api_key, p_id, repo_name, filename, repo_file, url, columns):
        dataverse_server = server
        api_key = api_key
        persistentId = p_id

        files = {'file': (url.split('/')[-1], open(filename, 'rb'))}
        desc = "Data snapshot from %s" % url
        cat = [repo_name.split('/')[1]]
        for col in columns:
            cat.append(col)
        params = dict(description=desc,
                  directoryLabel=repo_file,
                categories=cat)

        params_as_json_string = json.dumps(params)

        payload = dict(jsonData=params_as_json_string)

        url_persistent_id = '%s/api/datasets/:persistentId/add?persistentId=%s&key=%s' % (dataverse_server, persistentId, api_key)

        print('-' * 40)
        print('making request')
        r = requests.post(url_persistent_id, data=payload, files=files)

        print('-' * 40)
        try:
            print(r.json())
        except:
            print(r.content)
        print(r.status_code)
        return
    
    def collect_urls(self):
        contents = self.repo.get_contents("")
        DEBUG = False
        while contents:
            file_content = contents.pop(0)
            urlfullpath = "%s/%s/%s/%s" % (gitroot, self.REPO, gitblob, file_content.path)
            rawurl = "%s/%s/%s/%s" % (gituserroot, self.REPO, gitmaster, file_content.path)
            rawurl = rawurl.replace(' ', '%20')
            if file_content.type == "dir":
                contents.extend(self.repo.get_contents(file_content.path))
                continue
        
            if len(PARSABLE_EXTENSIONS) == 0 or file_content.name.split('.')[-1] in PARSABLE_EXTENSIONS:
                if DEBUG:
                    print("%s -> %s" % (urlfullpath, rawurl)) 
                self.urls_found[file_content.path] = rawurl

        print('Found {} URLs'.format(len(self.urls_found)))
        return self.urls_found

    def upload_files_to_dataverse(self, ds_id, urls_found):
        for file, url in urls_found.items():
            columns = []
            #for url in urls:
            if file:
                print(url)
                try:
                    tmpfile = urllib.request.urlretrieve(url) # retrieve the csv in a temp file, if there is a problem with the URL it throws and we continue
                except:
                    continue
            
                try:
                    filename = 'file://{}'.format(tmpfile[0])
                    # TODO: try gzipped datasets as well
                    #if not re.findall(r'(gz$|np$|nt$)', filename):
                    #    pd.read_csv(filename) # try reading it as csv, if fails continue
                    print("%s -> %s" % (filename, url))
                    if self.validate_df:
                         if re.search(r"(xls|xlsx)", url):
                             df = pd.read_excel(filename)
                             columns = list(df.columns)
                         elif re.search(r"json", url):
                             df = pd.read_excel(filename)
                             columns = list(df.columns)
                         else:
                             df = pd.read_csv(filename)
                             columns = list(df.columns)
                         if self.DEBUG:
                             print("Columns: %s" % df.columns)
                    metadata = self.make_file_metadata(REPO, file, url)
                    print('- uploading the following dataset {}'.format(url))
                except:
                    continue
        
                self.upload_datafile(BASE_URL, API_TOKEN, self.mapping_dsid2pid[ds_id], self.REPO, tmpfile[0], file, url, columns)
        return 


