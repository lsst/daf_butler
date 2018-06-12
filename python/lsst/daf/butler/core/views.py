# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import text, select, table
from sqlalchemy.ext import compiler

__all__ = ('makeView', )


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView)
def compileCreateView(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (element.name, compiler.sql_compiler.process(element.selectable))


@compiler.compiles(DropView)
def compileDropView(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)


def makeView(name, metadata, selectable):
    """Make a SQL view.

    Parameters
    ----------
    name : `str`
        Name of the view table.
    metadata : `sqlalchemy.Metadata`
        Metadata to attach the view to.
    selectable : `sqlalchemy.sql.select` or `str`
        The SQL ``SELECT`` clause used to generate the view.
    """
    t = table(name)

    if isinstance(selectable, str):
        selectable = select([text(selectable.lstrip("SELECT"))])

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, selectable).execute_at('after-create', metadata)
    DropView(name).execute_at('before-drop', metadata)

    return t
