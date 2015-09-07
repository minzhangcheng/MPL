# ############################################################################
#
# Copyright (C) 2015 Minzhang Cheng
# Contact: minzhangcheng@gmail.com
#
# This file is part of the Minzhang's Python Library, a Python library with many utils by Minzhang Cheng.
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
import ftplib
import requests


def ftpDownload(url, path, filename='', user='anonymous', password=''):

    """
    ##############################################################################
    #
    # ftpDownload(url,              URL of ftp, pointing to a file
    #             path,             The path to store downloaded file
    #             filename='',      Filename, default to use the original name from ftp server
    #             user='anonymous', FTP user, default to use anonymous mode
    #             password='')      FTP password, default to use anonymous mode
    #
    # Download one file from ftp server, with url like
    #                        [ftp://][user:password]@ftphost[:port]/[path/]filename
    #
    ##############################################################################
    """

    url = url.strip(' \t\n')
    if url[:6] == 'ftp://':
        url = url[6:]
    at = url.find('@')
    if at >= 0:
        (ftpUser, host) = url.rsplit('@', 1)
        user = ftpUser
        sep = ftpUser.find(':')
        if sep >= 0:
            (user, password) = ftpUser.split(':', 1)
    else:
        host = url
    (host, ftpPath) = host.split('/', 1)
    host = host.split(':')
    if len(host) == 2:
        port = host[1]
        host = host[0]
    else:
        port = 21
        host = host[0]
    sep = ftpPath.find('/')
    if sep >= 0:
        (ftpPath, name) = ftpPath.rsplit('/', 1)
    else:
        name = ftpPath
        ftpPath = ''
    if path[-1] != '/':
        path += '/'
    if filename == '':
        filename = path + name
    else:
        filename = path + filename
    ftp = ftplib.FTP()
    ftp.connect(host, port)
    ftp.login(user, password)
    if ftpPath != '':
        ftp.cwd(ftpPath)
    outFile = open(filename, 'wb')
    ftp.retrbinary('RETR %s' % name, outFile.write)
    ftp.quit()
    outFile.close()
    return True


def httpDownload(url, path, filename=''):

    """
    Download one file from http server.

    httpDownload(url,              URL of a file
                 path,             The path to store downloaded file
                 filename='')      Filename, default to use the original name
                                   from http server
    """

    if path[-1] not in '/':
        path += '/'
    if len(filename) == 0:
        file = url.rsplit('/', 1)[-1]
        file = path + file
    else:
        file = path + filename
    req = requests.get(url)
    outFile = open(file, 'wb')
    outFile.write(req.content)
    outFile.close()
    return True


def download(url, path, filename=''):
    """
    Download one file from remote server.

    download(url,             URL of a file
             path,            The path to store downloaded file
             filename='')     Filename, default to use the original name from
                              remote server
    """
    if url[:6] == 'ftp://':
        return ftpDownload(url, path, filename)
    else:
        return httpDownload(url, path, filename)