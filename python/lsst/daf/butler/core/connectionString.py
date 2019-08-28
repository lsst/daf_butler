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

__all__ = ("DB_AUTH_ENVVAR", "DB_AUTH_PATH", "ConnectionStringFactory")

from sqlalchemy.engine import url
from lsst.daf.butler.core.dbAuth import DbAuth
from lsst.daf.butler.core.registry import RegistryConfig

DB_AUTH_ENVVAR = "LSST_DB_AUTH"
"""Default name of the environmental variable that will be used to locate DB
credentials configuration file. """

DB_AUTH_PATH = "~/.lsst/db-auth.yaml"
"""Default path at which it is expected that DB credentials are found."""


class ConnectionStringFactory:
    """Factory for `ConnectionString` instances.

    Parameters
    ----------
    path : `str` or None, optional
        Path to configuration file. Defaults to '~/.lsst/daf-auth.yaml'.
    envVar : `str` or None, optional
        Name of environment variable pointing to configuration file. Defaults
        to "LSST_DB_AUTH"
    authList : `list` [`dict`] or None, optional
        Authentication configuration.

    Notes
    -----
    At least one of ``path``, ``envVar``, or ``authList`` must be provided;
    generally ``path`` should be provided as a default location.
    """

    keys = ('username', 'password', 'host', 'port', 'database')

    def __str__(self):
        return f"{self.__class__.__name__}@{self._dbAuth.authList}"

    def fromConfig(self, config):
        """Parses a Config 'db' string and other keywords and returns an
        `sqlalchemy.engine.url.URL` object containing the components of a URL
        used to connect to a database.

        Parameters
        ----------
        config : `Config` or `RegistryConfig`
            Config to parse for connection string components.
        dbAuth : `DbAuth` or None
            DbAuth instance pointing to the correct credentials file. If None
            the default "~/.lsst/db-auth.yaml" or "LSST_DB_AUTH" env var will
            be used.

        Returns
        -------
        conStr : `sqlalchemy.engine.url.URL`
            URL object suitable to be passed into `sqlalchemy.create_engine`
            function.
        """
        regConf = RegistryConfig(config)
        conStr = url.make_url(regConf['db'])

        for key in self.keys:
            if getattr(conStr, key) is None:
                setattr(conStr, key, regConf.get(key))

        # when the databsase is a file, host will be None and the path
        # becomes the database, but we are not treating 'localhost' explicitly
        host = conStr.host
        if conStr.host is None:
            host = str(conStr.database)

        if "sqlite" not in conStr.drivername:
            dbAuth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
            auth = dbAuth.getAuth(conStr.drivername, conStr.username, host,
                                  conStr.port, conStr.database)
            conStr.username = auth[0]
            conStr.password = auth[1]

        return conStr
