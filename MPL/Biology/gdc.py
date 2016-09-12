import __future__
import os
import multiprocessing
import time
import tempfile
import requests
import json


class Gdc:
    __gdcUrl = 'https://gdc-api.nci.nih.gov'
    __endpoints = {
        'project': 'projects',
        'file': 'files',
        'case': 'cases',
        'annotation': 'annotations',
        'mapping': '_mapping',
        'data': 'data'
    }

    def setUrl(self, url):
        Gdc.__gdcUrl = url

    def gdcUrl(self):
        return Gdc.__gdcUrl

    def setEndpoints(self, endpoints):
        Gdc.__endpoints = endpoints

    def endpoints(self):
        return Gdc.__endpoints

    def url(self, endpoint):
        return '%s/%s' % (self.gdcUrl(), self.endpoints()[endpoint])

    def find(self, endpoint, values, fields=list(), size=0, errorIgnore=False,
             maxTrial=10, wait=3, timeout=10, tested=0, log=None):
        if not maxTrial > tested:
            if errorIgnore:
                return []
            else:
                raise Exception
        url = self.url(endpoint)
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
            return self.find(endpoint, values, fields, size, errorIgnore,
                             maxTrial, wait, timeout, tested, log)
        r = response.json()
        print('%s\n%s\n\n' % (response.url, r), file=log)
        if 'data' not in r:
            tested += 1
            time.sleep(wait)
            return self.find(endpoint, values, fields, size, errorIgnore,
                             maxTrial, wait, timeout, tested, log)
        total = r['data']['pagination']['total']
        count = r['data']['pagination']['count']
        if size == 0 and total > count:
            return self.find(endpoint, values, fields, total, errorIgnore,
                             maxTrial, wait, timeout, tested, log)
        else:
            return r['data']['hits']


class Case(Gdc):
    def __init__(self, id):
        self.__id = id

    def setId(self, id):
        self.__id = id

    def Id(self):
        return self.__id

    def files(self, fields=list(), size=0, errorIgnore=False,
              maxTrial=10, wait=3, timeout=10, tested=0, log=None):
        values = {'cases.case_id': [self.__id], 'files.access': ['open']}
        return self.find('file', values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)


class Project(Gdc):
    def __init__(self, id):
        self.__id = id

    def setId(self, id):
        self.__id = id

    def Id(self):
        return self.__id

    def cases(self, fields=list(), size=0, errorIgnore=False,
              maxTrial=10, wait=3, timeout=10, tested=0, log=None):
        values = {'cases.project.project_id': [self.Id()]}
        return self.find('case', values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)

    def files(self, fields=list(), size=0, errorIgnore=False,
              maxTrial=10, wait=3, timeout=10, tested=0, log=None):
        values = {'cases.project.project_id': [self.Id()]}
        return self.find('file', values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)


class File(Gdc):
    def __init__(self, id):
        self.__id = id

    def setId(self, id):
        self.__id = id

    def Id(self):
        return self.__id

    def download(self, filename=None, maxTrial=10, wait=3,
                 timeout=30, tested=0, log=None):
        if not maxTrial > tested:
            raise(Exception)
        url = self.url('data')
        url = '%s/%s' % (url, self.Id())
        try:
            response = requests.get(url, timeout=timeout)
        except requests.RequestException:
            print('%s\n%s\n\n' % (response.url, 'Requests Error'), file=log)
            tested += 1
            time.sleep(wait)
            return self.download(filename, maxTrial, wait,
                                 timeout, tested, log)

        if not filename:
            filename = self.Id()
        print('%s\n%s\n\n' % (response.url, filename), file=log)
        wf = open(filename, 'wb')
        wf.write(response.content)
        wf.close()


"""
file = File('fd21fb6b-7a99-4677-98ab-4b1a97acd736')
filename = '/home/minzhang/%s' % file.Id()
file.download(filename)
"""


