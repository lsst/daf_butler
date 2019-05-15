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

__all__ = ("View", )

from sqlalchemy import MetaData
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import text, select, TableClause
from sqlalchemy.ext import compiler
from sqlalchemy import event


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView, 'postgresql')
def compileCreateView(element, compiler, **kw):
    return "CREATE OR REPLACE VIEW %s AS %s" % (element.name,
                                                   compiler.sql_compiler.process(element.selectable))

@compiler.compiles(CreateView)
def compileCreateView(element, compiler, **kw):
    return "CREATE VIEW IF NOT EXISTS %s AS %s" % (element.name,
                                                   compiler.sql_compiler.process(element.selectable))


# Ignoring PEP8 redefinition of function, as this is the sqlalchemy
# recommended procedure for dealing with multiple dialects
@compiler.compiles(CreateView, 'oracle')  # noqa: F811
def compileCreateView(element, compiler, **kw):
    # This is special cased as oracle does not support IF NOT EXISTS,
    # but should be kept until the consequences of using CREATE OR REPLACE
    # would be on users who are actively using a view when someone else
    # attempts to create or replace it.
    return """
DECLARE
    nCount NUMBER;
BEGIN
    SELECT COUNT(view_name) INTO nCount FROM user_views WHERE view_name = '{}';
    IF(nCount <= 0)
    THEN
        execute immediate 'CREATE VIEW {} AS {}';
    END IF;
END;
""".format(element.name,
           element.name,
           compiler.sql_compiler.process(element.selectable).replace(";\n FROM DUAL", ""))


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
