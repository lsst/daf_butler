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

__all__ = ("PostgreSqlRegistry", )

from sqlalchemy import create_engine
import sqlalchemy.dialects.postgresql.dml

from lsst.daf.butler.core.config import Config
from lsst.daf.butler.core.registryConfig import RegistryConfig

from .sqlRegistry import SqlRegistry, SqlRegistryConfig


class PostgreSqlRegistry(SqlRegistry):
    """Registry backed by an PostgreSQL Amazon RDS service.

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
        return create_engine(self.config.connectionString, pool_size=1)

    def _makeInsertWithConflict(self, table, onConflict):
        # Docstring inherited from SqlRegistry._makeInsertWithConflict.
        return self._makeInsertWithConflictImpl(table, onConflict)

    @staticmethod
    def _makeInsertWithConflictImpl(table, onConflict):
        """Implementation for `_makeInsertWithConflict`.

        This is done as a separate static method to facilitate testing.
        """
        # This uses special support for UPSERT in PostgreSQL backend:
        # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        query = sqlalchemy.dialects.postgresql.dml.insert(table)
        if onConflict == "ignore":
            query = query.on_conflict_do_nothing(constraint=table.primary_key)
        elif onConflict == "replace":
            # in SET clause assign all columns using special `excluded`
            # pseudo-table, if some column in a table does not appear in
            # INSERT list this will set it to NULL.
            excluded = query.excluded
            data = {column.name: getattr(excluded, column.name)
                    for column in table.columns
                    if column.name not in table.primary_key}
            query = query.on_conflict_do_update(constraint=table.primary_key, set_=data)
        else:
            raise ValueError(f"Unexpected `onConflict` value: {onConflict}")
        return query
