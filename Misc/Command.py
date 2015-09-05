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


import sys
import os
import subprocess

def addPATH(path, pathEnv='PATH', osType=None, sep=None):
    if sep is None:
        if osType is None:
            osType = sys.platform
        if osType[:3].lower() == 'win':
            sep = ';'
        else:
            sep = ':'
    prePATH = os.getenv(pathEnv)
    if prePATH:
        newPATH = '%s%s%s' % (path, sep, prePATH)
    else:
        newPATH = path
    os.putenv(pathEnv, newPATH)

def runCommand(command, cwd=None, path=None, timeout=None,
               pathEnv='PATH', osType=None):
    if path:
        addPATH(path, pathEnv, osType)
    if cwd:
        p = subprocess.Popen(command, cwd=cwd)
    else:
        p = subprocess.Popen(command)
    r = p.wait(timeout)
    if r != 0:
        return False
    else:
        return True
