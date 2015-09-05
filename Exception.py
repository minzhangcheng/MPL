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


class Error(Exception):
    pass


class Warning(Exception):
    pass


class NotSupported(Error):
    def __init__(self, wanted='', supported=list(), category=''):
        self.category = category
        self.supported = supported
        self.wanted = wanted

    def __str__(self):
        e = ''
        if self.category:
            e += 'Unsupported %s!\n\t' % self.category
        if not self.wanted:
            self.wanted = 'what you offered'
        e = 'We are sorry, but %s is not supported.\n\t' % self.wanted
        if self.supported:
            e += 'We currently support: '
            e += ', '.join(self.supported)
        return e
