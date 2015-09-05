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


import SQL
import Misc.Command

from SQL.SQL import selectQuery
from SQL.SQL import deleteQuery
from SQL.SQL import updateQuery
from SQL.SQL import insertQuery
from SQL.SQL import insertManyQuery


class MySQL(SQL.SQL):

    def __init__(self, *args, **kwargs):
        SQL.SQL.__init__(self, args, kwargs)
        if 'path' in kwargs:
            self.__desc.setdefault('path', kwargs['path'])
        if 'path' in self.__desc:
            Misc.Command.addPATH(self.__desc['path'])

    def __del__(self):
        SQL.SQL.__del__()


