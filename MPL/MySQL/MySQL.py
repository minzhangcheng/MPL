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


class MySQL(MPL.SQL.SQL):

    def __init__(self, *args, **kwargs):
        MPL.SQL.SQL.__init__(self, args, kwargs)
        if 'path' in kwargs:
            self.__desc.setdefault('path', kwargs['path'])
        if 'path' in self.__desc:
            MPL.Misc.Command.addPATH(self.__desc['path'])

    def __del__(self):
        MPL.SQL.SQL.__del__()

    def importSQL(self, sqlFile):
        cmd = 'mysql'



