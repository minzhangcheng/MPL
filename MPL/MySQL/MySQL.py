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

import MPL.SQL
import MPL.Misc.Command

from MPL.SQL.SQL import selectQuery
from MPL.SQL.SQL import deleteQuery
from MPL.SQL.SQL import updateQuery
from MPL.SQL.SQL import insertQuery
from MPL.SQL.SQL import insertManyQuery
from MPL.MySQL.Exception import *


class MySQL(MPL.SQL.SQL):

    __initKwargs = ['host', 'user', 'password', 'port', 'database',
                    'sqlConnector', 'sqlType', 'transaction', 'path']
    __mysqlLongKwargs = {'host': 'host',
                         'user': 'user',
                         'port': 'port'
    }

    def __init__(self, *args, **kwargs):
        MPL.SQL.SQL.__init__(self, args, kwargs)
        if 'path' in kwargs:
            self.__desc.setdefault('path', kwargs['path'])
        if 'path' in self.__desc:
            MPL.Misc.Command.addPATH(self.__desc['path'])

    def __del__(self):
        MPL.SQL.SQL.__del__()

    def __connectParameter(self):
        longPar = dict()
        for i in self.__mysqlLongKwargs:
            if i in self.__desc:
                if i in self.__sqldbKwargsNotStr:
                    longPar.setdefault(self.__mysqlLongKwargs[i], self.__desc[i])
                else:
                    longPar.setdefault(self.__mysqlLongKwargs[i],
                                       "'%s'" % self.__desc[i])
        return MPL.Misc.Command.parameterJoin([], {}, longPar, join='=')

    def runSqlScript(self, sqlFilename, outFilename=None, cwd=None,
                     timeout=0, byCMD=True):
        cmd = 'mysql %s < %s' % (self.__connectParameter(), sqlFilename)
        if outFilename:
            cmd += ' > %s' % outFilename
        return MPL.Misc.Command.runCommand(cmd, cwd=cwd, timeout=timeout)

    def query(self, query, autoCommit=None, byCMD=False, outFilename=None, timeout=0, cwd=None):
        if not byCMD:
            return MPL.SQL.SQL.query(self, query, autoCommit)
        cmd = 'echo "%s" | mysql %s' % (query, self.__connectParameter())
        if outFilename:
            cmd += ' > %s' % outFilename
        return MPL.Misc.Command.runCommand(cmd, cwd=cwd, timeout=timeout)

    def table2File(self, outFilename, database=None, table=None, timeout=0, cwd=None):
        if database is None:
            if 'database' in self.__desc:
                database = self.__desc['database']
            else:
                raise QueryError('Database not set.')
        if table is None:
            if 'table' in self.__desc:
                table = self.__desc['table']
            else:
                raise QueryError('Table not set.')
        query = 'SELECT * FROM %s;' % table
        return self.query(query, None, True, outFilename, timeout, cwd)


