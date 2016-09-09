import __future__
import os
import multiprocessing
import tempfile
import requests
import json


gdcUrl = 'https://gdc-api.nci.nih.gov/'
baseDir = '/home/minzhang/Desktop/GDC/'

endpoints = {
    'project': 'projects',
    'file': 'files',
    'case': 'cases',
    'annotation': 'annotations',
    'mapping': '_mapping'
}

def find(endpoint, values, fields=list(), size=0):
    url = 'https://gdc-api.nci.nih.gov/%s' % endpoints[endpoint]
    filter_content = [
        {
            'op': 'in',
            'content':
                {
                    'field': i,
                    'value': values[i]
                }
        } for i in values
    ]
    filters = {
        'op': 'and',
        'content': filter_content
    }
    params = {'filters': json.dumps(filters)}
    if size != 0:
        params['size'] = size
    if fields:
        params['fields'] = ','.join(fields)
    response = requests.get(url, params=params)
    r = response.json()
    if size == 0 and r['data']['pagination']['total'] > r['data']['pagination']['count']:
        return find(endpoint, values, fields, r['data']['pagination']['total'])
    else:
        return r['data']['hits']

def case2files(case, fields=list(), size=0):
    values = {'cases.case_id': [case], 'files.access': ['open']}
    return find('file', values, fields, size)

def project2cases(project, fields=list(), size=0):
    values = {'cases.project.project_id': [project]}
    return find('case', values, fields, size)

"""
files = case2files('012e99fe-e3e8-4bb0-bb74-5b0c9992187c')
wf = open(baseDir + 'Cases/012e99fe-e3e8-4bb0-bb74-5b0c9992187c.json', 'w')
print(json.dumps(files, indent=2), file=wf)
wf.close()

files = project2cases('TCGA-STAD', fields=['case_id'])
wf = open(baseDir + 'TCGA-STAD.json', 'w')
print(json.dumps(files, indent=2), file=wf)
wf.close()
"""

def printFileList(case):
    files = case2files(case)
    print(case)
    wf = open('/home/minzhang/Desktop/GDC/file_list/%s.json' % case, 'w')
    print(json.dumps(files, indent=2), file=wf)
    wf.close()


def findAllFiles(project):
    cases = [i['case_id'] for i in project2cases(project, ['case_id'])]
    pool = multiprocessing.Pool(processes=4)
    pool.map(printFileList, cases)

# findAllFiles('TCGA-STAD')





