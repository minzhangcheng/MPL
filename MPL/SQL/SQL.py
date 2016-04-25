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
import sys
import logging

import MPL.Misc
from MPL.SQL.Exception import *


def __whereState(keyValueDict, relation='AND'):
    if not keyValueDict:
        return ''
    if relation.upper() == 'AND':
        sep = ' AND '
    elif relation.upper() == 'OR':
        sep = ' OR '
    else:
        sep = relation
    where = list()
    for key in keyValueDict:
        where.append('%s = %s' % (key, keyValueDict[key]))
    w = sep.join(where)
    return 'WHERE %s' % w


def selectQuery(keyValueDict, targetColumns=list(), table=None):
    if not targetColumns:
        gc = '*'
    else:
        gc = ', '.join(targetColumns)
    w = __whereState(keyValueDict)
    q = "SELECT %s FROM %s\n\t%s;" % (gc, table, w)
    return q


def countQuery(self, keyValueDict, table=None):
    w = __whereState(keyValueDict)
    q = "SELECT COUNT(*) FROM %s\n\t%s;" % (table, w)


def deleteQuery(keyValueDict, table):
    w = __whereState(keyValueDict)
    q = "DELETE FROM %s\n\t%s;" % (table, w)
    return q


def updateQuery(keyValueDict, updateValueDict, table):
    w = __whereState(keyValueDict)
    s = __whereState(updateValueDict, ', ')
    q = "UPDATE %s SET %s\n\t%s;" % (table, s, w)
    return q


def insertQuery(insertValueDict, table):
    c, v = MPL.Misc.dict2Lists(insertValueDict)
    c = ', '.join(c)  # c: string for columns
    v = ', '.join(v)  # v: string for values
    q = "INSERT INTO %s (%s) VALUES \n\t(%s)\n;" % (table, c, v)
    return q


def __insertValueMany(insertValueListList):
    q = ''
    for i in insertValueListList:
        q += '\t(%s)\n' % (', '.join(i))
    q += ';'
    return q


def insertManyQuery(insertValueListDict, table, itemPerOneTime=500):
    queries = list()
    c, v = MPL.Misc.dict2Lists(insertValueListDict)
    c = ', '.join(c)  # c: string for columns
    vl = MPL.Misc.Table.transpose(v)
    begin = 0
    q = "INSERT INTO %s (%s) VALUES\n" % (table, c)
    while len(vl) - begin > itemPerOneTime:
        q += __insertValueMany(vl[begin:begin+itemPerOneTime])
        begin += itemPerOneTime
        queries.append(q)
        q = "INSERT INTO %s (%s) VALUES\n" % (table, c)
    q += __insertValueMany(vl[begin:begin+itemPerOneTime])
    queries.append(q)
    return queries


class SQL:

    """
    Class MPL.SQL.SQL offers a general API to manipulate SQL database, including
        MySQL, SQLite, PostgreSQL, Oracle and Microsoft SQL Server. We use the
        DB-API compact libraries in this class, and the library you want to be
        used should be installed first. The default DB-API compact libraries we
        choose could be found in MPL.SQL.Config.defaultSqlConnector, and you can
        use other libraries as well.

    Members:
        self.__desc       a dict to describe the database connection
        self.SQLdb      the object of the DB-API library
        self.shortName  short name of the connection, used in log and hints
        self.connected  whether this database has been connected or not
        self.connection the connection object in DB-API library, it would be
                            None if not connected
        self.cursor     the cursor object in DB-API library, it would be
                            None if not connected

    Main functions:

        __init__(self, user=None, password=None, host=None, port=None,
                 sqlDesc = None, database=None, log=None, warning=None,
                 sqlType='MySQL', sqlConnector=None)
            user:           username of database, if no username or for SQLite,
                                keep it as None
            password:       password of database, if no username or for SQLite,
                                keep it as None
            host:           hostname or IP address of database, for SQLite, use
                                the filename instead
            port:           port of database, for default port or SQLite, keep
                                it as None
            database:       the database name, for MySQL
            log:            file handler for log, keep it None if no log is need
            warning:        file handler to log warnings and errors, keep it as
                                None if no log is need
            sqlType:        the type of Database Manager System, it would be set
                                as 'MySQL' if kept as None
            sqlConnector:   the DB-API library you want to use, if kept as None,
                                the library would chosen automatically according
                                to MPL.SQL.Config.defaultSqlConnector
            sqlDesc:        a dict to describe the database, it looks like this:
                                defaultSqlHostDesc = {
                                    'host': 'localhost',
                                    'port': 3306,
                                    'user': 'user',
                                    'password': 'password',
                                    'sqlType': 'MySQL',
                                    'log': None,
                                    'warning': None
                                    }
                                WARNING: sqlDesc is the last in priority,all
                                    other arguments upon could replace the
                                    parameters set in sqlDesc

        connect(self)   connect to database

        commit(self, ignoreFailed=False)
                        commit to database (but not close the connection)
                        If commit failed, a error TransactionFailed would be
                            raised, but if ignoreFailed was set as True, no
                            exception would be raise.

        rollback(self, ignoreFailed=False)
                        rollback to database (but not close the connection)
                        If rollback failed, a error TransactionFailed would be
                            raised, but if ignoreFailed was set as True, no
                            exception would be raise.

        close(self, commit=False, ignoreFailed=True)
                        close connection. If commit set as True, it would try to
                            commit before close the connection, and if failed to
                            commit, exception would raise if ignoreFailed is not
                            set to be False.

        setUser(self, user, close=True, commit=False)

        def setPassword(self, password, close=True, commit=False)

        def setHost(self, host, close=True, commit=False)

        setPort(self, port, close=True, commit=False)

        setDesc(self, desc, close=True, commit=False)

        setDatabase(self, database, close=True, commit=False)

        setLog(self, log, close=True, commit=False)

        setWarning(self, warning, close=True, commit=False)

        query(self, query, autoCommit=True, resultLog=False)
                Query through the connection.
            query:      the query statement
            autoCommit: whether commit the query automatically
            resultLog:  whether append the query result in log file
            return:     (query result, query successfully or not)

        queryMany(self, queries, transaction=True,
                      autoCommit=True, resultLog=False)
                Query through the connection with many query statements.
            queries:        the list of query statements
            transaction:    default to be True. If one of the query failed, the
                                following queries would not continue to run,
                                and it would try to rollback the previous query.
                                If set as False, then it would go through the
                                failure, without rollback, and continue to run
                                the following queries.
            autoCommit:     whether commit the query automatically after all
                                queries finished
            resultLog:      whether append the query result in log file
            return:         (a list of each query result, whether all queries
                                are successful, a list of query successfully or
                                not for each query)

        find(self, table, keyValueDict, targetColumns=list(), resultLog=False)
                basic select function.
            table:          table name
            keyValueDict:   a dict like this to build where statement:
                                {column name 1: value, column name 2 value ...}
            targetColumns:  a list of target columns, kept as empty if you want
                                to get all columns
            resultLog:      whether append the query result in log file
            return:         query result

        count(self, table, keyValueDict, resultLog=False)
                count how many items are in the select results.
            table:          table name
            keyValueDict:   a dict like this to build where statement:
                                {column name 1: value, column name 2 value ...}
            targetColumns:  a list of target columns, kept as empty if you want
                                to get all columns
            resultLog:      whether append the query result in log file
            return:         item count

        exist(self, table, keyValueDict, resultLog=False)
                whether the target item exits.
            table:          table name
            keyValueDict:   a dict like this to build where statement:
                                {column name 1: value, column name 2 value ...}
            targetColumns:  a list of target columns, kept as empty if you want
                                to get all columns
            resultLog:      whether append the query result in log file
            return:         item count

        delete(self, table, keyValueDict, autoCommit=True, resultLog=False)
                delete target items.
            table:          table name
            keyValueDict:   a dict like this to build where statement:
                                {column name 1: value, column name 2: value ...}
            targetColumns:  a list of target columns, kept as empty if you want
                                to get all columns
            autoCommit:     whether commit the query automatically
            resultLog:      whether append the query result in log file

        update(self, table, keyValueDict, updateValueDict,
               autoCommit=True, resultLog=False)
                update items.
            table:              table name
            keyValueDict:       a dict like this to build where statement:
                                    {column name 1: value,
                                     column name 2: value ...}
            updateValueDict:    a dict like this to set new values:
                                    {column name 1: value,
                                     column name 2: value ...}
            autoCommit:     whether commit the query automatically
            resultLog:      whether append the query result in log file

        insert(self, table, insertValueDict, autoCommit=True, resultLog=False)
            table:              table name
            insertValueDict:    a dict like this to set values:
                                    {column name 1: value,
                                     column name 2: value ...}
            autoCommit:     whether commit the query automatically
            resultLog:      whether append the query result in log file

        insertMany(self, table, insertValueListDict, itemPerOneTime=500,
                   autoCommit=True, resultLog=False)
            table:                  table name
            insertValueListDict:    a dict like this to set values:
                                    {column name 1: [value 11, value 12 ...],
                                     column name 2: [value 21, value 22 ...]
                                     ...}
            itemPerOneTime:         how many items would be insert in one query
                                        statement
            autoCommit:     whether commit the query automatically
            resultLog:      whether append the query result in log file

        insertUpdate(self, table, keyValues, updateValues,
                     autoCommit=True, resultLog=False)
        insertIgnore(self, table, keyValues, updateValues,
                     autoCommit=True, resultLog=False)
                these two function would check the existence before insertion.
                    If item not exists, item would be inserted. If item exists,
                    insertUpdate would update the item with new values, and
                    insertIgnore would ignore the insert and do nothing.
            table:          table name
            keyValues:      a dict like this to build where statement:
                                    {column name 1: value,
                                     column name 2: value ...}
            updateValues:   a dict like this to set values:
                                    {column name 1: value,
                                     column name 2: value ...}
            autoCommit:     whether commit the query automatically
            resultLog:      whether append the query result in log file

    """

    __initKwargs = ['host', 'user', 'password', 'port', 'database', 'table',
                    'sqlConnector', 'sqlType', 'transaction', 'path']
    __standardLoggingLevels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    __sqlConnector = {
        'MySQL': 'pymysql',
        'MariaDB': 'pymysql',
        'SQLite': 'sqlite3',
        'PostgreSQL': '',
        'Oracle': '',
        'MSSQLServer': ''
    }
    __sqlTypeName = {
        'MySQL': ['MySQL'],
        'MariaDB': ['MariaDB'],
        'SQLite': ['SQLite', 'SQLite3'],
        'PostgreSQL': ['PostgreSQL', 'Postgre SQL', 'PGSQL'],
        'Oracle': ['Oracle'],
        'MSSQLServer': ['MSSQLServer', 'MicrosoftSQLServer',
                        'Microsoft SQL Server', 'SQLServer', 'SQL Server']
    }
    __sqldbKwargs = {'host': 'host',
                     'user': 'user',
                     'password': 'password',
                     'port': 'port',
                     'database': 'database'
    }
    __sqldbLinuxKwargs = {'host': 'host',
                          'user': 'usr',
                          'password': 'passwd',
                          'port': 'port',
                          'database': 'database'
    }
    __sqldbKwargsNotStr = ['port']

    def __shortName(self):
        shortName = '%s' % self.__desc['sqlConnector']
        if 'sqlType' in self.__desc:
            shortName += '(%s): ' % self.__desc['sqlType']
        else:
            shortName += ': '
        if 'user' in self.__desc:
            shortName += '%s' % self.__desc['user']
        if 'password' in self.__desc:
            shortName += ':%s' % self.__desc['password']
        if 'host' in self.__desc:
            shortName += '\@%s' % self.__desc['host']
        if 'port' in self.__desc:
            shortName += ':%d' % self.__desc['port']
        if 'database' in self.__desc:
            shortName += '(%s)' % self.__desc['database']
        return shortName

    def __connector(self, sqlType='MySQL'):
        for i in self.__sqlTypeName:
            for j in self.__sqlTypeName[i]:
                if sqlType[:len(j)].upper() == j.upper():
                    return self.__sqlConnector[i]
        else:
            raise SqlTypeNotSupported(wanted=sqlType,
                                      supported=self.__sqlConnector.keys())

    def __init__(self, *args, **kwargs):
        self.__desc = dict()
        if 'hostDesc' in kwargs:
            for i in kwargs['hostDesc']:
                self.__desc.setdefault(i, kwargs['hostDesc'][i])
        for i in range(0, len(args)):
            key = self.__initKwargs[i]
            value = args[i]
            if key not in self.__desc:
                self.__desc.setdefault(key, value)
            else:
                self.__desc[key] = value
        for i in kwargs:
            if i not in self.__desc:
                self.__desc.setdefault(i, kwargs[i])
            else:
                self.__desc[i] = kwargs[i]
        if 'logFile' in self.__desc:
            logging.basicConfig(filename=self.__desc['logFile'])
        if 'logLevel'not in self.__desc:
            self.__desc.setdefault('logLevel', 'WARNING')
        if self.__desc['logLevel'] not in self.__standardLoggingLevels:
            self.__desc['logLevel'] = 'WARNING'
        __setLog = 'logging.basicConfig(level=logging.%s)' \
                   % self.__desc['logLevel']
        exec(__setLog)

        if 'sqlConnector' not in self.__desc:
            if 'sqlType' in self.__desc:
                sqlType = self.__desc['sqlType']
                logging.debug('Try to guess the DB-API library compact with %s'
                              % sqlType)
                try:
                    self.__desc.setdefault('sqlConnector',
                                         self.__connector(sqlType))
                    logging.info('%s was chosen to be the DB-API library to '
                                 'connect to %s'
                                 % (self.__desc['sqlConnector'], sqlType))
                except SqlTypeNotSupported as e:
                    logging.critical(str(e))
                    sys.exit(2)
            else:
                self.__desc.setdefault('sqlConnector', self.__connector())
                logging.info('%s was chosen to be the DB-API library'
                             % self.__desc['sqlConnector'])
        self.SQLdb = None
        logging.info('Try to import library %s' % self.__desc['sqlConnector'])
        try:
            exec('import %s' % self.__desc['sqlConnector'])
            exec('self.SQLdb = %s' % self.__desc['sqlConnector'])
        except:
            error = 'Can not import module %s, check whether this ' \
                    'module has been installed into Python, please.' \
                    % self.__desc['sqlConnector']
            logging.critical(error)
            print(error)
            sys.exit(2)

        if 'path' in kwargs:
            self.__desc.setdefault('path', kwargs['path'])
        if 'path' in self.__desc:
            MPL.Misc.Command.addPATH(self.__desc['path'])

        self.shortName = self.__shortName()

        self.connected = False
        self.__connection = None
        self.__cursor = None

    def connect(self):
        comment = 'Connect to %s,' % self.shortName
        par = dict()
        for i in self.__sqldbKwargs:
            if i in self.__desc:
                if i in self.__sqldbKwargsNotStr:
                    par.setdefault(self.__sqldbKwargs[i], self.__desc[i])
                else:
                    par.setdefault(self.__sqldbKwargs[i],
                                   "'%s'" % self.__desc[i])
        parStr = MPL.Misc.Command.parameterJoin([], par, {}, shortPrefix='',
                                                join='=', sep=', ')
        conStr = 'self.SQLdb.connect(%s)' % parStr
        logging.debug('Try to connect to SQL database: %s' % conStr)
        con = None
        try:
            con = eval(conStr)
        except Exception as e:
            error = ConnectFailed(self.shortName, comment)
            logging.error(str(error))
            raise error
        print(con)
        logging.info('Connected to %s.' % self.shortName)
        self.__connection = con
        self.__cursor = self.__connection.cursor()
        self.connected = True
        logging.info('New connection to %s.' % self.shortName)
        return self

    def commit(self, ignoreFailed=False):
        query = 'commit'
        comment = 'Commit the to %s.'\
                  % self.shortName
        try:
            self.__cursor.commit()
        except Exception as e:
            warning = TransactionFailed(query, comment, self.shortName)
            logging.warning(str(warning))
            if not ignoreFailed:
                raise warning
        logging.info(comment)
        return self

    def rollback(self, ignoreFailed=False):
        query = 'rollback'
        comment = 'Rollback the to %s.'\
                  % self.shortName
        try:
            self.__cursor.rollback()
        except Exception as e:
            warning = TransactionFailed(query, comment, self.shortName)
            logging.warning(str(warning))
            if not ignoreFailed:
                raise warning
        logging.info(comment)
        return self

    def close(self, commit=False, ignoreFailed=True):
        if not self.connected:
            return self
        if commit:
            self.commit(ignoreFailed)
        elif 'transaction' in self.__desc and self.__desc['transaction']:
            self.rollback(ignoreFailed)
        self.__cursor.close()
        self.__connection.close()
        self.connected = False
        logging.info('The connection to %s has closed.' % self.shortName)
        self.connected = False
        return self

    def __del__(self):
        self.close()

    def setUser(self, user, close=True, commit=False, ignoreFalse=True):
        if 'user' in self.__desc:
            self.__desc['user'] = user
        else:
            self.__desc.setdefault('user', user)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setPassword(self, password, close=True, commit=False, ignoreFalse=True):
        if 'password' in self.__desc:
            self.__desc['password'] = password
        else:
            self.__desc.setdefault('password', password)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setHost(self, host, close=True, commit=False, ignoreFalse=True):
        if 'host' in self.__desc:
            self.__desc['host'] = host
        else:
            self.__desc.setdefault('host', host)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setPort(self, port, close=True, commit=False, ignoreFalse=True):
        if 'port' in self.__desc:
            self.__desc['port'] = port
        else:
            self.__desc.setdefault('port', port)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setDesc(self, desc, close=True, commit=False, ignoreFalse=True):
        for i in desc:
            if i not in self.__desc:
                self.__desc.setdefault(i, desc[i])
            else:
                self.__desc[i] = desc[i]
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setDatabase(self, database, close=True, commit=False, ignoreFalse=True):
        if 'database' in self.__desc:
            self.__desc['database'] = database
        else:
            self.__desc.setdefault('database', database)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def setLogLevel(self, logLevel, close=False, commit=False, ignoreFalse=True):
        if 'logLevel' in self.__desc:
            self.__desc['logLevel'] = logLevel
        else:
            self.__desc.setdefault('logLevel', logLevel)
        if close:
            self.close(commit, ignoreFalse)
        self.shortName = self.__shortName()

    def query(self, query, autoCommit=None):
        if not self.connected:
            self.connect()
        l = 'Query database %s' % self.shortName
        l += '\n\nQuery:\n'
        l += query
        logging.info(l)
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except self.SQLdb.Warning as w:
            logging.warning(str(w))
            return [], False
        except self.SQLdb.Error as e:
            logging.error(str(e))
            return [], False
        logging.debug('Result: %s' % str(result))
        if autoCommit:
            self.commit(False)
        else:
            if 'transaction' in self.__desc and not self.__desc['transaction']:
                self.commit(False)
        return result, True


    def queryMany(self, queries, transaction=None, autoCommit=None):
        if not self.connected:
            self.connect()
        results = list()
        states = list()
        for query in queries:
            state, result = self.query(query, False)
            if not state:
                if transaction or transaction is None \
                        and 'transaction' in self.__desc \
                        and self.__desc['transaction']:
                    self.rollback()
                elif autoCommit:
                    self.commit(True)
                return results, False, states
            states.append(state)
            results.append(result)
            if (transaction is not None and not transaction) or\
                    (transaction is None and 'transaction' in self.__desc and
                     self.__desc['transaction']):
                self.commit()
        if autoCommit:
            self.commit()
        return results, True, states


    def find(self, keyValueDict, targetColumns=list(), table=None):
        if not table:
            if 'table' in self.__desc:
                table = self.__desc['table']
            else:
                logging.critical('Table not set.')
                raise QueryError('Table not set.')
        q = selectQuery(keyValueDict, targetColumns, table)
        result, state = self.query(q, False)
        return result

    def count(self, keyValueDict, table=None):
        if not table:
            if 'table' in self.__desc:
                table = self.__desc['table']
            else:
                logging.critical('Table not set.')
                raise QueryError('Table not set.')
        q = countQuery(keyValueDict, table)
        result, state = self.query(q, False)
        return result[0][0]

    def exist(self, keyValueDict, table):
        c = self.count(keyValueDict, table)
        if c > 0:
            return True
        else:
            return False

    def delete(self, keyValueDict, table=None, autoCommit=True):
        q = deleteQuery(keyValueDict, table)
        result, state = self.query(q, autoCommit)
        return state

    def update(self, keyValueDict, updateValueDict, table=None,
               autoCommit=True):
        q = updateQuery(keyValueDict, updateValueDict, table)
        result, state = self.query(q, autoCommit)
        return state

    def insert(self, insertValueDict, table=None, autoCommit=True):
        q = insertQuery(insertValueDict, table)
        result, state = self.query(q, autoCommit)
        return state

    def insertMany(self, insertValueListDict, table=None, itemPerOneTime=500,
                   transaction=None, autoCommit=True):
        queries = insertManyQuery(insertValueListDict, table, itemPerOneTime)
        result, state, states = self.queryMany(queries, transaction, autoCommit)
        return state

    def insertUpdate(self, keyValues, updateValues, table=None,
                     autoCommit=True):
        if self.exist(keyValues, table):
            return self.update(keyValues, updateValues, table,
                               autoCommit)
        else:
            return self.insert(keyValues, table, autoCommit)

    def insertIgnore(self, keyValues, updateValues, table=None,
                     autoCommit=True):
        if self.exist(keyValues, table):
            return True
        else:
            return self.insert(updateValues, table, autoCommit)

