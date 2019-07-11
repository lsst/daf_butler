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

from lsst.daf.butler.core.registry import ConnectionStringBuilder

from .oracleRegistry import OracleRegistry


class PostgreSqlRegistry(OracleRegistry):
    """Registry backed by an PostgreSQL Amazon RDS service.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """
    dialect = 'postgres'
    driver = 'psycopg2'

    def _createEngine(self):
        return create_engine(ConnectionStringBuilder.fromConfig(self.config),
                             pool_size=1)
