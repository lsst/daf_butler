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

from sqlalchemy import MetaData
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import text, select, TableClause
from sqlalchemy.ext import compiler
from sqlalchemy import event

__all__ = ("View", )


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView)
def compileCreateView(element, compiler, **kw):
    return "CREATE VIEW IF NOT EXISTS %s AS %s" % (element.name,
                                                   compiler.sql_compiler.process(element.selectable))


@compiler.compiles(DropView)
def compileDropView(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)


class View(TableClause):
    """Make a SQL view.

    Parameters
    ----------
    name : `str`
        Name of the view table.
    metadata : `sqlalchemy.Metadata`
        Metadata to attach the view to.
    selectable : `sqlalchemy.sql.select` or `str`
        The SQL ``SELECT`` clause used to generate the view.
    comment : `str`, optional
        Text description of the view.  Unlike the comment attribute of
        actual `sqlalchemy.schema.SchemaItem` instances, this comment is
        not currently used when creating the view, but it may be in the
        future.
    info : `dict`, optional
        Arbitrary extra information describing the view.
    """

    def __init__(self, name, metadata, selectable, comment=None, info=None):
        super().__init__(name)
        self.comment = comment
        self.info = info

        if isinstance(selectable, str):
            selectable = select([text(selectable.lstrip("SELECT"))])

        for c in selectable.c:
            c._make_proxy(self)

        @event.listens_for(MetaData, "after_create")
        def receive_after_create(target, connection, **kwargs):
            connection.execute(CreateView(name, selectable))

        @event.listens_for(MetaData, "before_drop")
        def receive_before_drop(target, connection, **kwargs):
            connection.execute(DropView(name))
