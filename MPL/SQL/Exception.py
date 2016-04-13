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
import MPL.Exception


class SqlTypeNotSupported(MPL.Exception.NotSupported):
    """
    Exception for not supported database type.
    Subclass of MPL.Exception.NotSupported.
    Raise SqlTypeNotSupported(wanted='what_the_user_want') and it would
    generate the hint like this:
        Unsupported DBMS (database manager system) !
            We are sorry, but what_the_user_want is not supported.
            We currently support: MySQL, MariaDB, SQLite, PostgreSQL, Oracle,
            MSSQLServer.
    """
    def __init__(self, wanted='', supported=''):

        self.category = 'DBMS (database manager system) '
        self.supported = supported
        self.wanted = wanted


class ConnectFailed(MPL.Exception.Error):
    """
    Exception for database connection fails.
    """
    def __init__(self, target='', comment=''):
        self.target = target
        self.comment = comment

    def __str__(self):
        e = 'Connection Error.'
        if self.target or self.comment:
            if self.target:
                e += '\n\tCan not connect to %s' % self.target
                if self.comment:
                    e += ', with following details:\n%s' % self.comment
                else:
                    e+= '.'
            else:
                e += '\n\t%s' % self.comment
        return e


class TransactionFailed(MPL.Exception.Warning):
    """
    Exception for transaction fails.
    """
    def __init__(self, query='', comment='', connection=''):
        self.query = query
        self.comment = comment
        self.connection = connection

    def __str__(self):
        e = 'Error in query database % d.' % self.connection
        if self.query:
            e += '\nQuery command:'
            e += '\n%s' % self.query
        if self.comment:
            e += '\nComment:'
            e += '\n%s' % self.comment
        return e

class QueryError(MPL.Exception.Error):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        e = 'Query Error'
        e += self.message
        return e
