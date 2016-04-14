import __future__
import os
import tempfile
import requests
from Bio import SeqIO
from Bio import SwissProt
from Bio.Blast.Applications import NcbiblastpCommandline
from Bio.Blast import NCBIXML


# sp: swiss-prot formatted text

def fetchSwissProt(spAcc):
    url = 'http://www.uniprot.org/uniprot/%s.txt' % spAcc
    r = requests.get(url)
    code = r.status_code
    if code != 200:
        return ''
    else:
        return r.text


def fetchFasta(spAcc):
    url = 'http://www.uniprot.org/uniprot/%s.fasta' % spAcc
    r = requests.get(url)
    code = r.status_code
    if code != 200:
        return ''
    else:
        return r.text


def reviewedSp(sp):
    l = sp.split('\n')[0].split(' ')
    if "Reviewed;" in l:
        return True
    else:
        return False


def detail(sp):
    directory = tempfile.mkdtemp()
    spFile = '%s/seq.sp' % directory
    wf = open(spFile, 'w')
    print(sp, file=wf, sep='', end='')
    wf.close()
    seq_record = SeqIO.read(spFile, "swiss")
    os.remove(spFile)
    os.removedirs(directory)
    return seq_record.id, seq_record.name, seq_record.seq


def seq(sp):
    directory = tempfile.mkdtemp()
    spFile = '%s/seq.sp' % directory
    wf = open(spFile, 'w')
    print(sp, file=wf, sep='', end='')
    wf.close()
    seq_record = SeqIO.read(spFile, "swiss")
    os.remove(spFile)
    os.removedirs(directory)
    return seq_record.seq


def functionSp(sp):
    if sp:
        id, name, seq = detail(sp)
        directory = tempfile.mkdtemp()
        spFile = '%s/seq.sp' % directory
        wf = open(spFile, 'w')
        print(sp, file=wf, sep='', end='')
        wf.close()
        seq_record = SeqIO.read(spFile, "swiss")
        os.remove(spFile)
        os.removedirs(directory)
        if 'comment' in seq_record.annotations:
            cc = seq_record.annotations['comment'].split('\n')
            for line in cc:
                if line[:len('FUNCTION:')] == 'FUNCTION:':
                    return id, name, line[len('FUNCTION:'):]
    return '', '', ''


def blastpSp(sp, db, evalue=0.0001):
    directory = tempfile.mkdtemp()
    fastaFile = '%s/seq.fasta' % directory
    fasta = '>query\n%s' % seq(sp)
    wf = open(fastaFile, 'w')
    print(fasta, file=wf, sep='', end='')
    wf.close()
    blastp = NcbiblastpCommandline(query=fastaFile, db=db, evalue=evalue,
                                   outfmt=5, out='%s/result.xml' % directory)
    stdout, stderr = blastp()
    print(stdout, end='', sep='')
    print(stderr, end='', sep='')
    result_handle = open('%s/result.xml' % directory)
    blast_record = NCBIXML.read(result_handle)
    result_handle.close()
    os.remove(fastaFile)
    os.remove('%s/result.xml' % directory)
    os.removedirs(directory)
    hits = [align.title for align in blast_record.alignments]
    hits = [i.split('|')[1] for i in hits]
    return hits


def annotation(spAcc, db, evalue=0.0001):
    sp = fetchSwissProt(spAcc)
    if sp:
        id, name, seq = detail(sp)
        if reviewedSp(sp):
            return functionSp(sp)
        else:
            homolog = blastpSp(sp, db, evalue)
            if homolog:
                sp = fetchSwissProt(homolog[0])
                return functionSp(sp)
    return '', '', ''


"""
db = '/Users/minzhang/Downloads/swissprot/swissprot'
rf = open('/Users/minzhang/Desktop/spAcc.dat', 'r')
spAccs = [i.split('|')[1] for i in rf.readlines()]
wf = open('/Users/minzhang/Desktop/function.dat', 'w')
for i in spAccs:
    id, name, funct = annotation(i, db)
    print('%s\t%s\t%s\t%s' % (i, id, name, funct), file=wf, end='\n')
rf.close()
wf.close()
"""