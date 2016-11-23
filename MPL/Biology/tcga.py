import __future__
import os
import sys
import multiprocessing
import time
import tempfile
import requests
import json

null = 'NULL'
output_dir = '/home/minzhang/tcga'

with open('tcgaMap.json', 'r') as f:
    tcgaMap = json.load(f)
allProgramList = ['TCGA', 'TARGET']
gdcUrl = 'https://gdc-api.nci.nih.gov'
endpoints = {
    'project': 'projects',
    'file': 'files',
    'case': 'cases',
    'annotation': 'annotations',
    'mapping': '_mapping',
    'data': 'data'
}
fields = dict()
for ep in tcgaMap:
    f = set()
    for table in tcgaMap[ep]:
        for column in tcgaMap[ep][table]:
            f.add(tcgaMap[ep][table][column])
    fields[ep] = f
server = {
    'host': 'mysql.cmz.ac.cn',
    'port': 3306,
    'user': 'biodb_admin',
    'password': 'biodb_admin123456',
    'database': 'biodb'
}
data = dict()
columns = dict()
for i in tcgaMap:
    for j in tcgaMap[i]:
        if j not in columns:
            columns[j] = list(tcgaMap[i][j].keys())
        if j not in data:
            data[j] = dict()

def queryGdc(endpoint, values, fields=list(), size=0, errorIgnore=False,
             maxTrial=10, wait=3, timeout=10, tested=0, log=None):
    if not maxTrial > tested:
        if errorIgnore:
            return []
        else:
            raise Exception
    url = '%s/%s' % (gdcUrl, endpoints[endpoint])
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
    try:
        response = requests.get(url, params=params, timeout=timeout)
    except requests.RequestException:
        print('%s\n%s\n\n' % (response.url, 'Requests Error'), file=log)
        tested += 1
        time.sleep(wait)
        return queryGdc(endpoint, values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)
    r = response.json()
    print('%s\n%s\n\n' % (response.url, r), file=log)
    if 'data' not in r:
        tested += 1
        time.sleep(wait)
        return queryGdc(endpoint, values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)
    total = r['data']['pagination']['total']
    count = r['data']['pagination']['count']
    if size == 0 and total > count:
        return queryGdc(endpoint, values, fields, total, errorIgnore,
                         maxTrial, wait, timeout, tested, log)
    else:
        return r['data']['hits']


def list_project():
    project_ids = queryGdc('project', {'program.name': allProgramList}, ['project_id'])
    return [i['project_id'] for i in project_ids]


def get_project(project_ids):
    projects = queryGdc('project', {'project_id': project_ids}, fields['project'])
#     columns = {i: list(tcgaMap['project'][i].keys()) for i in tcgaMap['project']}
#     data = {i: dict() for i in tcgaMap['project']}

    for project in projects:
        d = list()
        # table project
        key = project['project_id']
        if key not in data['tcga_project']:
            for column in columns['tcga_project']:
                if column in project:
                    d.append(project[column])
                elif column == 'program_id':
                    d.append(project['program']['program_id'])
                else:
                    d.append(null)
            data['tcga_project'][key] = d

        d = list()
        # table program
        key = project['program']['program_id']
        if key not in data['tcga_program']:
            for column in columns['tcga_program']:
                if column in project['program']:
                    d.append(project['program'][column])
                else:
                    d.append(null)
            data['tcga_program'][key] = d


def list_case(projects):
    case_ids = queryGdc('case', {'project.project_id': projects}, ['case_id'])
    return [i['case_id'] for i in case_ids]


def get_case(case_ids):
    cases = queryGdc('case', {'case_id': case_ids}, fields['case'])
    upper_tables = {'tcga_project': 'project',
                    'tcga_tissue_source_site': 'tissue_source_site'
                    }
    simple_secondary_tables = {'tcga_demographic': 'demographic'
                               }
    multi_secondary_table = {'tcga_diagnosis': 'diagnoses',
                             'tcga_family_history': 'family_histories',
                             'tcga_exposure': 'exposures'
                             }
    for case in cases:

        for table in upper_tables:
            if upper_tables[table] in case:
                item = case[upper_tables[table]]
                d = list()
                key = item['%s_id' % table[5:]]
                if key != '' and key not in data[table]:
                    for column in columns[table]:
                        if column in item:
                            d.append(item[column])
                        elif column == 'project_id':
                            d.append(case['project']['project_id'])
                        elif column == 'case_id':
                            d.append(case['case_id'])
                        else:
                            d.append(null)
                    data[table][key] = d

        for table in simple_secondary_tables:
            if simple_secondary_tables[table] in case:
                item = case[simple_secondary_tables[table]]
                d = list()
                key = item['%s_id' % table[5:]]
                if key != '' and key not in data[table]:
                    for column in columns[table]:
                        if column in item:
                            d.append(item[column])
                        elif column == 'project_id':
                            d.append(case['project']['project_id'])
                        elif column == 'case_id':
                            d.append(case['case_id'])
                        else:
                            d.append('null')
                    data[table][key] = d

        for table in multi_secondary_table:
            if multi_secondary_table[table] in case:
                for item in case[multi_secondary_table[table]]:
                    d = list()
                    key = item['%s_id' % table[5:]]
                    if key != '' and key not in data[table]:
                        for column in columns[table]:
                            if column in item:
                                d.append(item[column])
                            elif column == 'project_id':
                                d.append(case['project']['project_id'])
                            elif column == 'case_id':
                                d.append(case['case_id'])
                            else:
                                d.append('null')
                        data[table][key] = d

        # treatment
        if 'diagnoses'in case:
            for dg in case['diagnoses']:
                dg_id = dg['diagnosis_id']
                if 'treatments' in dg:
                    for tr in dg['treatments']:
                        d = list()
                        key = tr['treatment_id']
                        if key != '' and key not in data['tcga_treatment']:
                            for column in columns['tcga_treatment']:
                                if column in item:
                                    d.append(item[column])
                                elif column == 'project_id':
                                    d.append(case['project']['project_id'])
                                elif column == 'case_id':
                                    d.append(case['case_id'])
                                elif column == 'diagnosis_id':
                                    d.append(dg_id)
                                else:
                                    d.append('null')
                            data['tcga_treatment'][key] = d

        item = case
        key = item['case_id']
        table = 'tcga_case'
        if key != '' and key not in data[table]:
            for column in columns[table]:
                if column in item:
                    d.append(item[column])
                elif column == 'project_id':
                    d.append(case['project']['project_id'])
                elif column == 'case_id':
                    d.append(case['case_id'])
                else:
                    d.append('null')
            data[table][key] = d


def list_file(case_ids):
    file_ids = queryGdc('file', {'cases.case_id': case_ids, 'data_category': 'Transcriptome Profiling', 'access': 'open'}, ['file_id'])
    return [i['file_id'] for i in file_ids]


def get_file(file_ids):
    files = queryGdc('file', {'file_id': file_ids}, fields['file'])
    for file in files:
        d = list()
        key = file['file_id']
        if key not in data['tcga_file_expression']:
            for column in columns['tcga_file_expression']:
                if column == 'comments':
                    if file['file_name'] == 'mirnas.quantification.txt':
                        d.append('miRNA')
                    elif 'FPKM-UQ' in file['file_name']:
                        d.append('Unique FPKM')
                    elif 'FPKM' in file['file_name']:
                        d.append('FPKM')
                    elif 'htseq.counts' in file['file_name']:
                        d.append('HTSeq Counts')
                    elif 'isoforms.quantification' in file['file_name']:
                        d.append('Isoform Quantification')
                    else:
                        d.append(null)
                elif column == 'case_id':
                    d.append(file['cases'][0]['case_id'])
                elif column == 'project_id':
                    d.append(file['cases'][0]['project']['project_id'])
                elif column in file:
                    d.append(file[column])
                else:
                    d.append(null)
            data['tcga_file_expression'][key] = d


def output_data(directory='.'):
    for table in data:
        if data[table]:
            wf = open('%s/%s.tsv' % (directory, table), 'w')
            col = columns[table]
            print('\t'.join(col), file=wf, end='\n')
            for line in data[table]:
                d = data[table][line]
                a = list()
                for i in d:
                    if i is None:
                        a.append(null)
                    else:
                        a.append(str(i))
                d = a
                print('\t'.join(d), file=wf, end='\n')
            wf.close()


def all(output_dir):
    projects = list_project()
    for project in projects:
        cases = list_case([project])
        case_group = [[]]
        n = 0
        for case in cases:
            if n > 9:
                case_group.append([])
                n = 0
            case_group[-1].append(case)
            n += 1
        for i in case_group:
            get_case(i)
            files = list_file(i)
            get_file(files)
    output_data(output_dir)


"""
get_project(['TCGA-SARC', 'TCGA-PAAD'])
get_case(['0004d251-3f70-4395-b175-c94c2f5b1b81', '000d566c-96c7-4f1c-b36e-fa2222467983', '005669e5-1a31-45fb-ae97-9d450e74e7cb'])
print(columns)
print(data)
directory = '/Users/minzhang/Desktop/tcga'
output_data(directory)
get_file(['77e73cc4-ff31-449e-8e3c-7ae5ce57838c'])
print(columns['tcga_file_expression'])
print(data['tcga_file_expression'])
print(queryGdc('file', {'file_id': ['77e73cc4-ff31-449e-8e3c-7ae5ce57838c'], ''}, ['access']))
files = list_file(['0004d251-3f70-4395-b175-c94c2f5b1b81', '000d566c-96c7-4f1c-b36e-fa2222467983', '005669e5-1a31-45fb-ae97-9d450e74e7cb'])
get_file(files)
print(data['tcga_file_expression'])
print(columns['tcga_file_expression'])
"""

all(output_dir)

