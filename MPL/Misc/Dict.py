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


def dict2Lists(input, sort=False):
    keys = input.keys()
    if sort:
        keys.sort()
    values = list()
    for i in keys:
        values.append(input[i])
    return keys, values


def dict2Tuples(input, sort=False):
    tuples = list()
    keys = input.keys()
    if sort:
        keys.sort()
    for i in keys:
        tuples.append((i, input[i]))
    return tuples


def unionDict(dicts, mode='union'):
    """mode: 'union' or 'intersection'"""
    d = dict()
    if mode == 'intersection':
        l = [len(i) for i in dicts]
        s = l.index(max(l))
        for k in dicts[s]:
            keep = True
            for i in dicts:
                if k not in i:
                    keep = False
                    break
            if keep:
                d.setdefault(k, [i[k] for i in dicts])
    if mode == 'union':
        for i in range(len(dicts)):
            for k in dicts[i]:
                if k not in d:
                    v = [[] for j in range(0,i)]
                    for j in dicts[i:]:
                        if k in j:
                            v.append(j[k])
                        else:
                            v.append([])
                    d.setdefault(k, v)
    return d