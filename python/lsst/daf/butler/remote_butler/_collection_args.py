# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

import re
from typing import cast

from ..registry import CollectionArgType
from ..registry.wildcards import CollectionWildcard
from .server_models import CollectionList


def convert_collection_arg_to_glob_string_list(arg: CollectionArgType) -> CollectionList:
    """Convert the collections argument used by many Butler/registry methods to
    a format suitable for sending to Butler server.

    Parameters
    ----------
    arg : `CollectionArgType`
        Collection search pattern in many possible formats.

    Returns
    -------
    glob_list : `CollectionList`
        Collection search patterns normalized to a list of globs string.

    Raises
    ------
    TypeError
        If the search pattern provided by the user cannot be converted to a
        glob string.
    """
    if arg is ...:
        return CollectionList(["*"])
    elif isinstance(arg, str):
        return CollectionList([arg])
    elif isinstance(arg, re.Pattern):
        raise TypeError("RemoteButler does not support regular expressions as search patterns")
    elif isinstance(arg, CollectionWildcard):
        # In theory this could work, but CollectionWildcard eagerly converts
        # any glob strings to regexes.  So it would need some rework to
        # preserve the original string inputs.  CollectionWildcard has likely
        # never been used in the wild by an end-user so this probably isn't
        # worth figuring out.
        raise TypeError("RemoteButler does not accept CollectionWildcard instances as search patterns")
    else:
        search = list(arg)
        for item in search:
            if not isinstance(item, str):
                raise TypeError("RemoteButler only accepts strings and lists of strings as search patterns")
        return CollectionList(cast(list[str], search))
