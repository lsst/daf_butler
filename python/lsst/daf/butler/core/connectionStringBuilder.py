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

__all__ = ("ConnectionStringBuilder", )

from sqlalchemy.engine import url
from lsst.daf.butler.core.registry import RegistryConfig
from lsst.daf.persistence import DbAuth


class ConnectionStringBuilder:
    """Builds a connection string by parsing (in order of precedence):

    1. 'db' key in RegistryConfig and
    2. RegistryConfig for explicitly defined keys,
    3. and ~/.lsst/db-auth.paf file.

    The 'db' string must be a connection URL, i.e. must start with a database
    dialect and contain, at least, the `database` component or the host name.

    Components `username`, `password`, `host`, `port` or `database` can also be
    specified explicitly as keys in the config when not part of the `db` string
    in Config.
    When `username` and `password` are not part of the `db` string or stated
    explicitly in Config they will be read from `~/.lsst/db-auth.paf` file by
    matching matching `host` and `port` components of provided URL.
    """
    keys = ('username', 'password', 'host', 'port', 'database')

    @classmethod
    def fromConfig(cls, config):
        """Parses a Config 'db' string and other keywords and returns an
        `sqlalchemy.engine.url.URL` object containing the components of a URL
        used to connect to a database.

        Parameters
        ----------
        config : `Config` or `RegistryConfig`
            Config to parse for connection string components.

        Returns
        -------
        conStr : `sqlalchemy.engine.url.URL`
            URL object suitable to be passed into `sqlalchemy.create_engine`
            function.
        """
        regConf = RegistryConfig(config)
        conStr = url.make_url(regConf['db'])

        for key in cls.keys:
            if getattr(conStr, key) is None:
                setattr(conStr, key, regConf.get(key))

        # DBAuth type requirement, for now
        host, port = str(conStr.host), str(conStr.port)
        # if DB is not a host but a file, does this case exist?
        if conStr.host is None:
            host = str(conStr.database)

        if DbAuth.available(host, port):
            if conStr.username is None:
                conStr.username = DbAuth.username(host, port)
            if conStr.password is None:
                conStr.password = DbAuth.password(host, port)

        return conStr
