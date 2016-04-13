import __future__
import sqlite3
import MPL.Misc.Table
import MPL.Misc.String
import MPL.Misc.Dict


def file2TableHead(infile, sep='\t', comment='#', blank='\n', header=True):
    head = []
    while header:
        head = infile.readline()
        head = head.strip(blank)
        if head[0] in comment:
            continue
        else:
            head = head.split(sep)
            header = False
    tb = MPL.Misc.Table.file2Table(infile, sep, comment, blank, header)
    return tb, head


def file2Sqlite(infile, db, tb, sep='\t', comment='#',
                blank='\n', header=True):
    table, head = file2TableHead(infile, sep, comment, blank, header)
    if not head:
        head = ['col%d' for d in range(len(table[0]))]
    con = sqlite3.connect(db)
    cur = con.cursor()
    if not head:
        head = ['col%d' for d in range(len(table[0]))]
    q = 'CREATE TABLE %s (%s);' % (tb, ', '.join(head))
    cur.execute(q)
    for i in table:
        q = "INSERT INTO %s VALUES ('%s');"\
            % (tb, "', '".join([MPL.Misc.String.stringValidate(j, '\'"\\')
                                for j in i]))
        print(q)
        cur.execute(q)
    con.commit()
    con.close()


def mergeTable(tbs, keys, values, mode='union'):
    """mode: 'union' or 'intersection'"""
    dicts = list()
    for i in range(len(tbs)):
        dicts.append(MPL.Misc.Table.table2Dict(tbs[i], keys[i], values[i]))
    return MPL.Misc.Dict.unionDict(dicts, mode)


def mergeFile2Dict(infiles, keys, values, sep='\t', comment='#', blank='\n',
              header=False, outfile=None, prefix=[], mode='union'):
    tbs = []
    heads = []
    for infile in infiles:
        tb, head = file2TableHead(infile, sep, comment, blank, header)
        tbs.append(tb)
        heads.append(head)
    merged = mergeTable(tbs, keys, values)
    if outfile:
        if header:
            head = []
            head.extend([heads[0][i] for i in keys[0]])
            for i in range(len(infiles)):
                if prefix:
                    head.extend(['%s%s' % (prefix[i], heads[i][j])
                                 for j in values[i]])
                else:
                    head.extend([heads[i][j] for j in values[i]])
            print(sep.join(head), end='\n', file=outfile)
        b = [['' for j in range(len(i))] for i in values]
        for k in merged:
            line = list()
            line.extend(k)
            v = merged[k]
            for i in range(len(v)):
                if v[i]:
                    line.extend(v[i])
                else:
                    line.extend(b[i])
            print(sep.join(line), end='\n', file=outfile)
    return merged


rf1 = open('/Users/minzhang/Downloads/GPL10558.annot')
rf2 = open('/Users/minzhang/Downloads/GSE66475_series_matrix.txt')
wf = open('/Users/minzhang/Downloads/GSE66475', 'w')
k = [[0], [0]]
v = [[1,2], [1,2,3,4,5,6,7,8,9,10,11]]
me = mergeFile2Dict([rf1, rf2], k, v, header=True, outfile=wf)
rf1.close()
rf2.close()
wf.close()
