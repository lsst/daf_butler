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

import logging

from lsst.daf.butler.core.utils import iterable
try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None


# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""


class Log:

    _initialized = False
    _lsstLogger = None
    _componentsInitialzied = []

    longLogFmt = "%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c (%X{LABEL})(%F:%L)- %m%n"
    normalLogFmt = "%c %p: %m%n"

    @staticmethod
    def getPyLogLevel(level):
        """Get the numeric value for the given log level name.

        Parameters
        ----------
        level : `str`
            One of the python `logging` log level names.

        Returns
        -------
        numericValue : `int`
            The python `logging` numeric value for the log level.
        """
        return getattr(logging, level, None)

    @staticmethod
    def getLsstLogLevel(level):
        """Get the numeric value for the given log level name.

        If `lsst.log` is not setup this function will return `None` regardless
        of input. `daf_butler` does not directly depend on `lsst.log` and so it
        will not be setup when `daf_butler` is setup. Packages that depend on
        `daf_butler` and use `lsst.log` may setup `lsst.log`.

        Will adapt the python name to an `lsst.log` name:
        - CRITICAL to FATAL
        - WARNING to WARN

        Parameters
        ----------
        level : `str`
            One of the python `logging` log level names.

        Returns
        -------
        numericValue : `int` or `None`
            The `lsst.log` numeric value.
        """
        if lsstLog is None:
            return None
        if level == "CRITICAL":
            level = "FATAL"
        elif level == "WARNING":
            level = "WARN"
        return getattr(lsstLog.Log, level, None)

    @classmethod
    def initLog(cls, longlog):
        """Initialize logging. This should only be called once per program
        execution. After the first call this will log a warning and return.

        If lsst.log is importable, will add its log handler to the python
        root logger's handlers.

        Parameters
        ----------
        longlog : `bool`
            If True, make log messages appear in long format, by default False.
        """
        if cls._initialized:
            # Unit tests that execute more than one command do end up
            # calling this fucntion multiple times in one program execution,
            # so do warn but don't log an error or fail, just make the
            # re-initalization a no-op.
            log = logging.getLogger(__name__.partition(".")[2])
            log.warning("Log is already initialized, returning without re-initializing.")
            return
        cls._initialized = True

        lgr = logging.getLogger()
        if lsstLog is not None:
            # global logging config

            lsstLog.configure_prop(_LOG_PROP.format(cls.longLogFmt if longlog else cls.normalLogFmt))

            cls._lsstLogger = lsstLog.LogHandler()
            # Forward all Python logging to lsstLog
            lgr.addHandler(cls._lsstLogger)
        logging.basicConfig()

        # also capture warnings and send them to logging
        logging.captureWarnings(True)

    @classmethod
    def uninitLog(cls):
        """Uninitialize the butler CLI Log handler.

        If the lsst.log handler was added to the python root logger's handlers
        in `initLog`, it will be removed here.

        For each loger level that was set by this class, sets that logger's
        level to NOTSET.
        """
        if cls._lsstLogger is not None:
            lgr = logging.getLogger()
            lgr.removeHandler(cls._lsstLogger)
        for component in cls._componentsInitialzied:
            logger = logging.getLogger(component)
            logger.setLevel(logging.NOTSET)

    @classmethod
    def setLogLevels(cls, logLevels):
        """Set log level for one or more components or the root logger.

        Parameters
        ----------
        longlog : `bool`
            If True then make log messages appear in "long format"
        logLevels : `list` of `tuple`
            per-component logging levels, each item in the list is a tuple
            (component, level), `component` is a logger name or an empty string
            or `None` for root logger, `level` is a logging level name, one of
            CRITICAL, ERROR, WARNING, INFO, DEBUG (case insensitive).
        """

        if isinstance(logLevels, dict):
            logLevels = logLevels.items()

        # configure individual loggers
        for component, level in logLevels:
            cls._componentsInitialzied.append(component)
            if lsstLog is not None:
                logger = lsstLog.Log.getLogger(component or "")
                logger.setLevel(cls.getLsstLogLevel(level))
            logging.getLogger(component or None).setLevel(cls.getPyLogLevel(level))
