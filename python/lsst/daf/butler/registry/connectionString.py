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
from .dbAuth import DbAuth, DbAuthError, DbAuthPermissionsError

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
            If the credentials file has incorrect permissions.
        """
        # this import can not live on the top because of circular import issue
        from lsst.daf.butler.registry import RegistryConfig
        regConf = RegistryConfig(registryConfig)
        conStr = url.make_url(regConf['db'])

        for key in cls.keys:
            if getattr(conStr, key) is None:
                setattr(conStr, key, regConf.get(key))

        # when host is None we cross our fingers and return
        if conStr.host is None:
            return conStr

        # allow other mechanisms to insert username and password by not forcing
        # the credentials to exist. If other mechanisms are used it's possible
        # that credentials were never set-up, or that there would be no match
        # in the credentials file. Both need to be ignored.
        try:
            dbAuth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
            auth = dbAuth.getAuth(conStr.drivername, conStr.username, conStr.host,
                                  conStr.port, conStr.database)
        except DbAuthPermissionsError:
            # re-raise permission error for safety
            raise
        except DbAuthError:
            # credentials file doesn't exist or no match was found
            pass
        else:
            conStr.username = auth[0]
            conStr.password = auth[1]

        return conStr
