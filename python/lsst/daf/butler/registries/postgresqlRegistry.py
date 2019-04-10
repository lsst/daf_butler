__all__ = ("PostgreSqlRegistry", )

import re
import configparser
import urllib.parse as urlparse
import os

from sqlalchemy import event
from sqlalchemy import create_engine

from lsst.daf.butler.core.config import Config
from lsst.daf.butler.core.registry import RegistryConfig

from .oracleRegistry import OracleRegistry
from .sqlRegistry import SqlRegistryConfig

class PostgreSqlRegistry(OracleRegistry):
    """Registry backed by an PostgreSQL Amazon RDS service. Everything
    is the same as Oracle registry, PostgreSQL requires quoting too:
        https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
    Inherited just for naming purposes and messing with the setConfigRoot thing.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set filesystem dependent config options for this Registry.
        There are no such options currently.
        """
        # must override the Registry skypix class and level
        super().setConfigRoot(root, config, full)

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False,
                 butlerRoot=None):
        registryConfig = SqlRegistryConfig(registryConfig)
        self.schemaConfig = schemaConfig
        super().__init__(registryConfig, schemaConfig, dimensionConfig, create,
                         butlerRoot=butlerRoot)

    def _createEngine(self):
        # its hard to say why ignoreQuote and _oracleExecute couldn't be methods
        # instead of nested functions. Ill just duplicate them here instead of rewriting
        # oracleReg just in case theres a no-no.
        tables = self.schemaConfig['tables'].keys()

        def _ignoreQuote(name, matchObj):
            """ This function conditionally adds quotes around a given name
            in a block of text if it has not already been quoted by either
            single or double quotes
            """
            text = matchObj.group(0)
            if text.startswith("'") and text.endswith("'"):
                return text
            elif text.startswith('"') and text.endswith('"'):
                return text
            else:
                return text.replace(name, f'"{name}"')

        def _oracleExecute(connection, cursor, statement, parameters, context, executemany):
            """ This function compares the text of a sql query against a known
            list of table names. If any name is found in the statement, the
            name is replaced with a quoted version of that name if it is
            not already quoted.
            """
            for name in tables:
                if name in statement:
                    # Regex translated: Optional open double quote, optional
                    # single quote table name, word boundary, optional close
                    # single quote, optional close double quote
                    statement = re.sub(f"\"?'?{name}\\b'?\"?", lambda x: _ignoreQuote(name, x), statement)
            return statement, parameters

        # I have slightly overcomplicated things here because I'm not sure what's wanted
        # once someone makes a decission about what is expected to be given as db string
        # then delete whats not neccessary.
        constr = self.config['db']

        # if both username and pass are in the db string - all is well
        # if username and pass are a nick that exists in a local conf file
        # read username and password from there
        parsed = urlparse.urlparse(constr)
        if parsed.username and (parsed.password is None):
            localconf = configparser.ConfigParser()
            localconf.read(os.path.expanduser('~/.rds/credentials'))

            username = localconf[parsed.username]['username']
            password = localconf[parsed.username]['password']
            constr = constr.replace(parsed.username, f'{username}:{password}')

        engine = create_engine(constr, pool_size=1)
        event.listen(engine, "before_cursor_execute", _oracleExecute, retval=True)
        return engine
