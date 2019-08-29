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
    """Factory for `sqlalchemy.engine.url.URL` instances.

    The factory constructs a connection string URL object by parsing the
    connection string, the 'db' key in the registry configuration.
    Username, password, host, port or database can be specified as keys in the
    config explicitly. If username or password are missing a matching DB is
    found in the credentials file pointed to by `DB_AUTH_ENVVAR` or
    `DB_AUTH_PATH` values.
    """

    keys = ('username', 'password', 'host', 'port', 'database')

    @classmethod
    def fromConfig(cls, registryConfig):
        """Parses the 'db' key in the config, and if they exist username,
        password, host, port and database keys, and returns an connection
        string object.

        If no  username and password are found in the connection string, or in
        the config, they are retrieved from a file at `DB_AUTH_PATH` or
        `DB_AUTH_ENVVAR`. Sqlite dialect does not require a password.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration

        Returns
        -------
        connectionString : `sqlalchemy.engine.url.URL`
            URL object representing the connection string.


        Raises
        ------
        DBAuthError
            If the credentials file has incorrect permissions, doesn't exist at
            the given location or is formatted incorrectly.
        """
        regConf = RegistryConfig(registryConfig)
        conStr = url.make_url(regConf['db'])

        for key in cls.keys:
            if getattr(conStr, key) is None:
                setattr(conStr, key, regConf.get(key))

        # when the databsase is a file, host will be None and the path
        # becomes the database, but we are not treating 'localhost' explicitly
        host = conStr.host
        if conStr.host is None:
            host = str(conStr.database)

        # sqlite with users and passwords not supported
        if None in (conStr.username, conStr.password) and "sqlite" not in conStr.drivername:
            dbAuth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
            auth = dbAuth.getAuth(conStr.drivername, conStr.username, host,
                                  conStr.port, conStr.database)
            conStr.username = auth[0]
            conStr.password = auth[1]

        return conStr
