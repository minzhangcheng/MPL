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


def stringValidate(string, invalidChar='"\\\'', transChar='\\'):

    """
    This function intend to check a string to see whether is has invalid
    characters, usually including ", \, and '. If yes, trans-meaning character,
    generally a back splash '\' would be insert before these characters.

    stringValid(string,                   string to be checked
                invalidChar = '"\\\'',    invalid characters
                transChar = '\\')         trans-meaning character
    """

    check = False
    for c in string:
        if c in invalidChar:
            check = True
            break
    if check:
        chList = list(string)
        i = 0
        while i < len(chList):
            if chList[i] in invalidChar:
                chList.insert(i, transChar)
                i += 2
            else:
                i += 1
        return ''.join(chList)
    else:
        return string


def splitStrip(string, sep, blank=' \t\n', max_sep=-1):
    """
    This function joins str.split() followed by str.strip(), it would split a
    string into a list of string, and then strip the blank character in each
    string.

    splitStrip(string,         the original string
               sep,            seperating charactors
               blank=' \t\n',  blank charactors, default to be ' ', '\t' and '\n'
               max_sep=-1)     the max seperation count, default to be no limitation
    """
    text = string.split(sep, max_sep)
    return [i.strip(blank) for i in text]
