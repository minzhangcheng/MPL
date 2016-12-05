import __future__
import os
import multiprocessing.dummy
import requests
import json
import time
# import tempfile
# import pymysql

null = 'NULL'

output_dir = '/home/minzhang/tcga'
download_dir = '/home/minzhang/tcga/files'
sql = '/home/minzhang/tcga/insert.sql'
expr = '/home/minzhang/tcga/expr.sql'

"""
output_dir = '/Users/minzhang/Desktop/tcga'
download_dir = '/Users/minzhang/Desktop/tcga/files'
sql = '/Users/minzhang/Desktop/tcga/insert.sql'
"""
thread = 16

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
             maxTrial=10, wait=10, timeout=10, tested=0, log=None):
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
        print(params, file=log)
        print('%s\n%s\n\n' % (url, 'Requests Error'), file=log)
        tested += 1
        time.sleep(wait)
        return queryGdc(endpoint, values, fields, size, errorIgnore,
                         maxTrial, wait, timeout, tested, log)
    r = response.json()
    print(params, file=log)
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
    upper_tables = {# 'tcga_project': 'project',
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
                            if item[column]:
                                d.append(item[column])
                            else:
                                d.append(null)
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
                            if item[column]:
                                d.append(item[column])
                            else:
                                d.append(null)
                        elif column == 'project_id':
                            d.append(case['project']['project_id'])
                        elif column == 'case_id':
                            d.append(case['case_id'])
                        else:
                            d.append(null)
                    data[table][key] = d

        for table in multi_secondary_table:
            if multi_secondary_table[table] in case:
                for item in case[multi_secondary_table[table]]:
                    d = list()
                    key = item['%s_id' % table[5:]]
                    if key != '' and key not in data[table]:
                        for column in columns[table]:
                            if column in item:
                                if item[column]:
                                    d.append(item[column])
                                else:
                                    d.append(null)
                            elif column == 'project_id':
                                d.append(case['project']['project_id'])
                            elif column == 'case_id':
                                d.append(case['case_id'])
                            else:
                                d.append(null)
                        data[table][key] = d

        # treatment
        if 'diagnoses'in case:
            for dg in case['diagnoses']:
                dg_id = dg['diagnosis_id']
                if 'treatments' in dg:
                    for tr in dg['treatments']:
                        d = list()
                        key = tr['treatment_id']
                        item = tr
                        if key != '' and key not in data['tcga_treatment']:
                            for column in columns['tcga_treatment']:
                                if column in item:
                                    if item[column]:
                                        d.append(item[column])
                                    else:
                                        d.append(null)
                                elif column == 'project_id':
                                    d.append(case['project']['project_id'])
                                elif column == 'case_id':
                                    d.append(case['case_id'])
                                elif column == 'diagnosis_id':
                                    d.append(dg_id)
                                else:
                                    d.append(null)
                            data['tcga_treatment'][key] = d

        item = case
        d = list()
        key = item['case_id']
        table = 'tcga_case'
        if key != '' and key not in data[table]:
            for column in columns[table]:
                if column in item:
                    if item[column]:
                        d.append(item[column])
                    else:
                        d.append(null)
                elif column == 'project_id':
                    d.append(case['project']['project_id'])
                elif column == 'case_id':
                    d.append(case['case_id'])
                elif column == 'tissue_source_site_id':
                    if 'tissue_source_site' in case:
                        d.append(case['tissue_source_site']['tissue_source_site_id'])
                    else:
                        d.append(null)
                else:
                    d.append(null)
            data[table][key] = d


def list_file(case_ids):
    file_ids = queryGdc('file', {'cases.case_id': case_ids,
                                 'data_category': ['Transcriptome Profiling', 'DNA Methylation'], 'access': 'open'},
                        ['file_id'])
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


def get_case_file(case_ids):
    get_case(case_ids)
    files = list_file(case_ids)
    if files:
        get_file(files)


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


def stringValidate(string, invalidChar='"\\\'', transChar='\\'):

    """
    This function intend to check a string to see whether is has invalid
    characters, usually including ", \, and '. If yes, trans-meaning character,
    generally a back splash '\' would be insert before these characters.
    stringValid(string,                   string to be checked
                invalidChar = '"\\\'',    invalid characters
                transChar = '\\')         trans-meaning character
    """
    if not string:
        return string
    check = False
    for c in string:
        if c in invalidChar:
            check = True
            break
    if check:
        chList = list(string)
        i = 0
        while i < len(chList):
            if chList[i] in invalidChar:
                chList.insert(i, transChar)
                i += 2
            else:
                i += 1
        return ''.join(chList)
    else:
        return string


def insert(table, columns, values, cur, log=None):
    if values:
        q = 'INSERT INTO %s (%s) VALUES\n' % (table, ', '.join(columns))
        v = []
        for i in values:
            l = []
            for j in i:
                if j is None:
                    l.append(null)
                elif j == null:
                    l.append(j)
                else:
                    l.append("'%s'" % stringValidate(str(j)))
            v.append(l)
        q += ',\n'.join(['\t(%s)' % ', '.join(i) for i in v])
        q += '\n;'
        print(q, file=log)
        if cur:
            cur.execute(q)
            cur.execute('COMMIT;')


def get_all_cases_files(maxCount=10):
    projects = list_project()
    get_project(projects)
    for project in projects:
        cases = list_case([project])
        case_group = [[]]
        n = 1
        for case in cases:
            if n > maxCount:
                case_group.append([])
                n = 1
            case_group[-1].append(case)
            n += 1
        #for l in case_group:
        #   get_case_file(l)
        with multiprocessing.dummy.Pool(thread) as p:
            p.map(get_case_file, case_group)


def insert_data(host='127.0.0.1', user='biodb_admin', passwd='biodb_admin123456', port=3306, database='biodb', maxInsert=100, log=None):
    tables = ['tcga_program', 'tcga_project', 'tcga_tissue_source_site', 'tcga_case', 'tcga_demographic', 'tcga_diagnosis', 'tcga_treatment', 'tcga_family_history', 'tcga_exposure', 'tcga_file_expression']
    for table in tables:
        column = columns[table]
        value = data[table]
        value_group = [[]]
        n = 1
        for v in value:
            if n > maxInsert:
                value_group.append([])
                n = 1
            value_group[-1].append(value[v])
            n += 1
        import pymysql
        con = pymysql.connect(host='127.0.0.1', user='biodb_admin', passwd='biodb_admin123456', port=3306,
                              database='biodb')
        cur = con.cursor()
        for v in value_group:
            insert(table, column, v, cur, log=log)
        cur.close()
        con.close()
        """
        def _insert_(vl):
            insert(table, column, vl, None, log=log)
        with multiprocessing.dummy.Pool(thread) as p:
            p.map(_insert_, value_group)
        """


def download_files(file_ids, file_names, download_dir, maxTrial=10, tested=0):
    if tested >= maxTrial:
        raise Exception
    tested += 1
    os.chdir(download_dir)
    d = '&'.join(['ids=%s' % i for i in file_ids])
    print(d)
    command = "curl --remote-name --remote-header-name --request POST 'https://gdc-api.nci.nih.gov/data' --data '%s'" % d
    r = os.system(command)
    if r != 0:
        return download_files(file_ids, file_names, download_dir, maxTrial, tested)
    if len(file_ids) > 1:
        r = os.system('tar xvf gdc_download_*.tar.gz')
        os.system('rm gdc_download_*.tar.gz')
        if r != 0:
            return download_files(file_ids, file_names, download_dir, maxTrial, tested)
        os.system('rm MANIFEST.txt')
        # r = ['%s/%s/%s' % (download_dir, file_ids[i], file_names[i]) for i in range(0, len(file_ids))]
    else:
        os.mkdir('%s/%s' %(download_dir, file_ids[0]))
        os.rename('%s/%s' % (download_dir, file_names[0]), '%s/%s/%s' %(download_dir, file_ids[0], file_names[0]))
        # r = ['%s/%s/%s' %(download_dir, file_ids[0], file_names[0])]
    r = list()
    for i in range(0, len(file_ids)):
        if file_names[i][-2:] == 'gz':
            os.chdir('%s/%s' % (download_dir, file_ids[i]))
            os.system('gzip -d %s' % file_names[i])
            r.append('%s/%s/%s' % (download_dir, file_ids[i], file_names[i][:-3]))
        else:
            r.append('%s/%s/%s' % (download_dir, file_ids[i], file_names[i]))
    return r


def insert_expr(ids, file_ids, file_names, cur=None, log=None):
    if len(ids) != len(file_ids) or len(ids) != len(file_names) or len(ids) == 0:
        return
    filenames = download_files(file_ids, file_names, download_dir)
    for i in range(0, len(ids)):
        values = list()
        with open(filenames[i], 'r') as f:
            for line in f.readlines():
                p = line.strip('\n\t ').split('\t')
                if len(p) == 2:
                    values.append([ids[i], p[0], p[1]])
        insert('tcga_expression', ['file_id', 'gene_id', 'value'], values, cur=cur, log=log)


def insert_expr_all(log=None):
    q = "SELECT id, file_id, file_name\n"
    q += "FROM tcga_file_expression\n"
    q += "WHERE comments in (%s)\n;" % ', '.join(["'miRNA'", "'FPKM'", "'Unique FPKM'", "'HTSeq Counts'"])
    print(q)
    import pymysql
    con = pymysql.connect(host='127.0.0.1', user='biodb_admin', passwd='biodb_admin123456', port=3306, database='biodb')
    cur = con.cursor()
    cur.execute(q)
    r = cur.fetchall()
    group = [[]]
    n = 1
    for i in r:
        if n > 10:
            group.append([])
            n = 1
        group[-1].append(i)
        n += 1
    """
    def _insert_(item):
        ids = [j[0] for j in item]
        file_ids = [j[1] for j in item]
        file_names = [j[2] for j in item]
        insert_expr(ids, file_ids, file_names, log)
    with multiprocessing.dummy.Pool(thread) as p:
        p.map(_insert_, group)
    """
    for i in group:
        ids = [j[0] for j in i]
        file_ids = [j[1] for j in i]
        file_names = [j[2] for j in i]
        insert_expr(ids, file_ids, file_names, cur, log)
    cur.close()
    con.close()


get_all_cases_files()
output_data(output_dir)
wf = open(sql, 'w')
insert_data(log=wf)
wf.close()


wf=open(expr, 'w')
insert_expr_all(log=wf)
wf.close()


print('end')