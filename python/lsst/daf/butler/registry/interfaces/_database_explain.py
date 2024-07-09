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

from typing import Any

from sqlalchemy import ClauseElement, Connection, Executable
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler


def get_query_plan(connection: Connection, sql: ClauseElement) -> str:
    """Retrieve the query plan for a given statement from the DB as a
    human-readable string.

    Parameters
    ----------
    connection : `sqlalchemy.Connection`
        Database connection used to retrieve query plan.
    sql : `sqlalchemy.ClauseElement`
        SQL statement for which we will retrieve a query plan.
    """
    if connection.dialect.name != "postgresql":
        # This could be implemented for SQLite using its EXPLAIN QUERY PLAN
        # syntax, but the result rows are a little different and we haven't had
        # a need for it yet.
        return "(not available)"

    with connection.execute(_Explain(sql)) as explain_cursor:
        lines = explain_cursor.scalars().all()
        return "\n".join(lines)


# This is based on code from the sqlalchemy wiki at
# https://github.com/sqlalchemy/sqlalchemy/wiki/Query-Plan-SQL-construct
class _Explain(Executable, ClauseElement):
    """Custom SQLAlchemy construct for retrieving query plan from the DB.

    Parameters
    ----------
    statement : `sqlalchemy.ClauseElement`
        SQLAlchemy SELECT statement to retrieve query plan for.
    """

    def __init__(self, statement: ClauseElement) -> None:
        self.statement = statement


@compiles(_Explain, "postgresql")
def _compile_explain(element: _Explain, compiler: SQLCompiler, **kw: Any) -> str:
    text = "EXPLAIN (VERBOSE TRUE, SETTINGS TRUE)"
    text += compiler.process(element.statement, **kw)

    return text
