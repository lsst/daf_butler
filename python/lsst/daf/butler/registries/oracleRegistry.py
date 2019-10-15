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

__all__ = ("OracleRegistry", )

from sqlalchemy import create_engine
from sqlalchemy.ext import compiler
from sqlalchemy.sql import ClauseElement, and_, bindparam, select
from sqlalchemy.sql.expression import Executable

from lsst.daf.butler.core.config import Config
from lsst.daf.butler.core.registryConfig import RegistryConfig

from .sqlRegistry import SqlRegistry, SqlRegistryConfig


class _Merge(Executable, ClauseElement):
    def __init__(self, table, onConflict):
        self.table = table
        self.onConflict = onConflict


@compiler.compiles(_Merge, "oracle")
def _merge(merge, compiler, **kw):
    """Generate MERGE query for inserting or updating records.
    """
    table = merge.table
    preparer = compiler.preparer

    allColumns = [col.name for col in table.columns]
    pkColumns = [col.name for col in table.primary_key]
    nonPkColumns = [col for col in allColumns if col not in pkColumns]

    # To properly support type decorators defined in core/schema.py we need
    # to pass column type to `bindparam`.
    selectColumns = [bindparam(col.name, type_=col.type).label(col.name) for col in table.columns]
    selectClause = select(selectColumns)

    tableAlias = table.alias("t")
    tableAliasText = compiler.process(tableAlias, asfrom=True, **kw)
    selectAlias = selectClause.alias("d")
    selectAliasText = compiler.process(selectAlias, asfrom=True, **kw)

    condition = and_(
        *[tableAlias.columns[col] == selectAlias.columns[col] for col in pkColumns]
    )
    conditionText = compiler.process(condition, **kw)

    query = f"MERGE INTO {tableAliasText}" \
            f"\nUSING {selectAliasText}" \
            f"\nON ({conditionText})"
    if merge.onConflict == "replace":
        updates = []
        for col in nonPkColumns:
            src = compiler.process(selectAlias.columns[col], **kw)
            dst = compiler.process(tableAlias.columns[col], **kw)
            updates.append(f"{dst} = {src}")
        updates = ", ".join(updates)
        query += f"\nWHEN MATCHED THEN UPDATE SET {updates}"
    elif merge.onConflict != "ignore":
        raise ValueError(f"Unexpected `onConflict` value: {merge.onConflict}")

    insertColumns = ", ".join([preparer.format_column(col) for col in table.columns])
    insertValues = ", ".join([compiler.process(selectAlias.columns[col], **kw) for col in allColumns])

    query += f"\nWHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({insertValues})"
    return query


class OracleRegistry(SqlRegistry):
    """Registry backed by a Oracle database.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    @classmethod
    def setConfigRoot(cls, root, config, full, overwrite=True):
        """Set any filesystem-dependent config options for this Registry to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from ``full`` to ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the value
            already exists.  Default is always to overwrite with the provided
            ``root``.

        Notes
        -----
        If a keyword is explicitly defined in the supplied ``config`` it
        will not be overridden by this method if ``overwrite`` is `False`.
        This allows explicit values set in external configs to be retained.
        """
        super().setConfigRoot(root, config, full, overwrite=overwrite)
        Config.updateParameters(RegistryConfig, config, full,
                                toCopy=("db",), overwrite=overwrite)

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False,
                 butlerRoot=None):
        registryConfig = SqlRegistryConfig(registryConfig)
        self.schemaConfig = schemaConfig
        super().__init__(registryConfig, schemaConfig, dimensionConfig, create,
                         butlerRoot=butlerRoot)

    def _createEngine(self):
        engine = create_engine(self.config.connectionString, pool_size=1)
        conn = engine.connect()
        # Work around SQLAlchemy assuming that the Oracle limit on identifier
        # names is even short than it is after 12.2.
        oracle_ver = engine.dialect._get_server_version_info(conn)
        engine.dialect.max_identifier_length = 128 if oracle_ver >= (12, 2) else 30
        conn.close()
        return engine

    def _makeInsertWithConflict(self, table, onConflict):
        # Docstring inherited from SqlRegistry._makeInsertWithConflict.
        return _Merge(table, onConflict)
