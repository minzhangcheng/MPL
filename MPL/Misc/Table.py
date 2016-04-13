# ############################################################################
#
# Copyright (C) 2015 Minzhang Cheng
# Contact: minzhangcheng@gmail.com
#
# This file is part of the Minzhang's Python Library, a Python library with
# many utils by Minzhang Cheng.
#
# GNU Lesser General Public License Usage
# This file may be used under the terms of the GNU Lesser General Public
# License version 3 as published by the Free Software Foundation and
# appearing in the file LICENSE included in the packaging of this file.
# Please review the following information to ensure the GNU Lesser
# General Public License version 3 requirements will be met:
# http://www.gnu.org/licenses/gpl-3.0.html
#
# ############################################################################


import __future__


def file2Table(infile, sep='\t', comment='#', blank='\n', header=False):
    """
    Read CSV-like file into a table (list of list), return a List of List.

    file2Table(infile,        readable file pointer
               sep='\t',      separation characters
               comment='#',   comment characters, the lines begins with which
                              would be ignored
               blank='\n',    blank characters, which would be deleted
               header=False)  whether file has a header, default to be False
    """

    table = list()
    for line in infile.readlines():
        if line[0] not in comment:
            if header:
                header = False
            else:
                table.append(line.strip(blank).split(sep))
    return table


def transpose(table):
    """
    Transpose a table (list of list).

    transpose(table)
    """
    return [[r[col] for r in table] for col in range(len(table[0]))]


def part(table, rowList=list(), colList=list()):
    """
    Get part of the table, only contains selected rows by columns. returns a
    List of List.

    part(table,           original table
         rowList=list()   the selected rows,
                          default to be all rows: range(0, len(table))
         colList=list())  the selected columns,
                          default to be all columns: range(0,len(table[0]))
    """
    if len(rowList) == 0:
        rowList = range(0, len(table))
    if len(colList) == 0:
        colList = range(0,len(table[0]))
    return [[table[i][j] for j in colList] for i in rowList]


def getRow(table, row):
    """
    Get a single row from a table, return a List.

    getRow(table, original table
           row)   selected row
    """
    return part(table, rowList=[row])[0]


def getColumn(table, col):
    """
    Get a single column from a table, return a List.

    getColumn(table, original table
              col)   selected column
    """
    return part(table, colList=[col])[0]


def table2Dict(tb, keys, values):
    d = dict()
    for i in tb:
        if tuple([i[j] for j in keys]) not in d:
            d.setdefault(tuple([i[j] for j in keys]), [i[j] for j in values])
    return d