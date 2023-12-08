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

__all__ = ("DB_AUTH_ENVVAR", "DB_AUTH_PATH", "ConnectionStringFactory")

from typing import TYPE_CHECKING

from lsst.utils.db_auth import DbAuth, DbAuthNotFoundError
from sqlalchemy.engine import url

if TYPE_CHECKING:
    from ._config import RegistryConfig

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

    keys = ("username", "password", "host", "port", "database")

    @classmethod
    def fromConfig(cls, registryConfig: RegistryConfig) -> url.URL:
        """Parse the `db`, and, if they exist, username, password, host, port
        and database keys from the given config.

        If no  username and password are found in the connection string, or in
        the config, they are retrieved from a file at `DB_AUTH_PATH` or
        `DB_AUTH_ENVVAR`. Sqlite dialect does not require a password.

        The `db` key value of the given config specifies the default connection
        string, such that if no additional connection string parameters are
        provided or retrieved, the `db` key is returned unmodified.

        Parameters
        ----------
        registryConfig : `RegistryConfig`
            Registry configuration.

        Returns
        -------
        connectionString : `sqlalchemy.engine.url.URL`
            URL object representing the connection string.

        Raises
        ------
        DbAuthPermissionsError
            If the credentials file has incorrect permissions.
        DbAuthError
            A problem occurred when retrieving DB authentication.

        Notes
        -----
        Matching requires dialect, host and database keys. If dialect is not
        specified in the db string and host and database keys are not found in
        the db string, or as explicit keys in the config, the matcher returns
        an unchanged connection string.
        Insufficiently specified connection strings are interpreted as an
        indication that a 3rd party authentication mechanism, such as Oracle
        Wallet, is being used and therefore are left unmodified.
        """
        # this import can not live on the top because of circular import issue
        from ._config import RegistryConfig

        regConf = RegistryConfig(registryConfig)
        conStr = url.make_url(regConf["db"])

        for key in cls.keys:
            if getattr(conStr, key) is None:
                # check for SQLAlchemy >= 1.4
                if hasattr(conStr, "set"):
                    conStr = conStr.set(**{key: regConf.get(key)})
                else:
                    # SQLAlchemy 1.3 or earlier, mutate in place
                    setattr(conStr, key, regConf.get(key))

        # Allow 3rd party authentication mechanisms by assuming connection
        # string is correct when we can not recognize (dialect, host, database)
        # matching keys.
        if any((conStr.drivername is None, conStr.host is None, conStr.database is None)):
            return conStr

        # Ignore when credentials are not set up, or when no matches are found
        try:
            dbAuth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
            auth = dbAuth.getAuth(
                conStr.drivername, conStr.username, conStr.host, conStr.port, conStr.database
            )
        except DbAuthNotFoundError:
            # credentials file doesn't exist or no matches were found
            pass
        else:
            # only assign auth when *no* errors were raised
            conStr = conStr.set(username=auth[0], password=auth[1])

        return conStr
