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

import datetime
import logging

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None

from lsst.daf.butler import ButlerMDC
from ..core.logging import JsonFormatter


class PrecisionLogFormatter(logging.Formatter):
    """A log formatter that issues accurate timezone-aware timestamps."""

    converter = datetime.datetime.fromtimestamp

    use_local = True
    """Control whether local time is displayed instead of UTC."""

    def formatTime(self, record, datefmt=None):
        """Format the time as an aware datetime."""
        ct = self.converter(record.created, tz=datetime.timezone.utc)
        if self.use_local:
            ct = ct.astimezone()
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.isoformat(sep='T', timespec='milliseconds')
        return s


class CliLog:
    """Interface for managing python logging and ``lsst.log``.

    This class defines log format strings for the log output and timestamp
    formats. It also configures ``lsst.log`` to forward all messages to
    Python `logging`.

    This class can perform log uninitialization, which allows command line
    interface code that initializes logging to run unit tests that execute in
    batches, without affecting other unit tests. See ``resetLog``."""

    defaultLsstLogLevel = lsstLog.FATAL if lsstLog is not None else None

    pylog_longLogFmt = "{levelname} {asctime} {name} ({MDC[LABEL]})({filename}:{lineno}) - {message}"
    """The log format used when the lsst.log package is not importable and the
    log is initialized with longlog=True."""

    pylog_normalFmt = "{name} {levelname}: {message}"
    """The log format used when the lsst.log package is not importable and the
    log is initialized with longlog=False."""

    configState = []
    """Configuration state. Contains tuples where first item in a tuple is
    a method and remaining items are arguments for the method.
    """

    _initialized = False
    _componentSettings = []

    _fileHandlers = []
    """Any FileHandler classes attached to the root logger by this class
    that need to be closed on reset."""

    @classmethod
    def initLog(cls, longlog: bool, log_tty: bool = True, log_file=()):
        """Initialize logging. This should only be called once per program
        execution. After the first call this will log a warning and return.

        If lsst.log is importable, will add its log handler to the python
        root logger's handlers.

        Parameters
        ----------
        longlog : `bool`
            If True, make log messages appear in long format, by default False.
        log_tty : `bool`
            Control whether a default stream handler is enabled that logs
            to the terminal.
        log_file : `tuple` of `str`
            Path to files to write log records. If path ends in ``.json`` the
            records will be written in JSON format. Else they will be written
            in text format. If empty no log file will be created. Records
            will be appended to this file if it exists.
        """
        if cls._initialized:
            # Unit tests that execute more than one command do end up
            # calling this function multiple times in one program execution,
            # so do log a debug but don't log an error or fail, just make the
            # re-initialization a no-op.
            log = logging.getLogger(__name__)
            log.debug("Log is already initialized, returning without re-initializing.")
            return
        cls._initialized = True
        cls._recordComponentSetting(None)

        if lsstLog is not None:
            # Ensure that log messages are forwarded back to python.
            # Disable use of lsst.log MDC -- we expect butler uses to
            # use ButlerMDC.
            lsstLog.configure_pylog_MDC("DEBUG", MDC_class=None)

            # Forward python lsst.log messages directly to python logging.
            # This can bypass the C++ layer entirely but requires that
            # MDC is set via ButlerMDC, rather than in lsst.log.
            lsstLog.usePythonLogging()

        if not log_tty:
            logging.basicConfig(force=True, handlers=[logging.NullHandler()])
        elif longlog:

            # Want to create our own Formatter so that we can get high
            # precision timestamps. This requires we attach our own
            # default stream handler.
            defaultHandler = logging.StreamHandler()
            formatter = PrecisionLogFormatter(fmt=cls.pylog_longLogFmt, style="{")
            defaultHandler.setFormatter(formatter)

            logging.basicConfig(level=logging.INFO,
                                force=True,
                                handlers=[defaultHandler],
                                )

        else:
            logging.basicConfig(level=logging.INFO, format=cls.pylog_normalFmt, style="{")

        # Initialize root logger level.
        cls._setLogLevel(None, "INFO")

        # also capture warnings and send them to logging
        logging.captureWarnings(True)

        # Create a record factory that ensures that an MDC is attached
        # to the records. By default this is only used for long-log
        # but always enable it for when someone adds a new handler
        # that needs it.
        ButlerMDC.add_mdc_log_record_factory()

        # Set up the file logger
        for file in log_file:
            handler = logging.FileHandler(file)
            if file.endswith(".json"):
                formatter = JsonFormatter()
            else:
                if longlog:
                    formatter = PrecisionLogFormatter(fmt=cls.pylog_longLogFmt, style="{")
                else:
                    formatter = logging.Formatter(fmt=cls.pylog_normalFmt, style="{")
            handler.setFormatter(formatter)
            logging.getLogger().addHandler(handler)
            cls._fileHandlers.append(handler)

        # remember this call
        cls.configState.append((cls.initLog, longlog, log_file))

    @classmethod
    def resetLog(cls):
        """Uninitialize the butler CLI Log handler and reset component log
        levels.

        If the lsst.log handler was added to the python root logger's handlers
        in `initLog`, it will be removed here.

        For each logger level that was set by this class, sets that logger's
        level to the value it was before this class set it. For lsst.log, if a
        component level was uninitialized, it will be set to
        `Log.defaultLsstLogLevel` because there is no log4cxx api to set a
        component back to an uninitialized state.
        """
        if lsstLog:
            lsstLog.doNotUsePythonLogging()
        for componentSetting in reversed(cls._componentSettings):
            if lsstLog is not None and componentSetting.lsstLogLevel is not None:
                lsstLog.setLevel(componentSetting.component or "", componentSetting.lsstLogLevel)
            logger = logging.getLogger(componentSetting.component)
            logger.setLevel(componentSetting.pythonLogLevel)
        cls._setLogLevel(None, "INFO")

        ButlerMDC.restore_log_record_factory()

        # Remove the FileHandler we may have attached.
        root = logging.getLogger()
        for handler in cls._fileHandlers:
            handler.close()
            root.removeHandler(handler)

        cls._fileHandlers.clear()
        cls._initialized = False
        cls.configState = []

    @classmethod
    def setLogLevels(cls, logLevels):
        """Set log level for one or more components or the root logger.

        Parameters
        ----------
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
            cls._setLogLevel(component, level)
            # remember this call
            cls.configState.append((cls._setLogLevel, component, level))

    @classmethod
    def _setLogLevel(cls, component, level):
        """Set the log level for the given component. Record the current log
        level of the component so that it can be restored when resetting this
        log.

        Parameters
        ----------
        component : `str` or None
            The name of the log component or None for the root logger.
        level : `str`
            A valid python logging level.
        """
        cls._recordComponentSetting(component)
        if lsstLog is not None:
            lsstLogger = lsstLog.Log.getLogger(component or "")
            lsstLogger.setLevel(cls._getLsstLogLevel(level))
        logging.getLogger(component or None).setLevel(cls._getPyLogLevel(level))

    @staticmethod
    def _getPyLogLevel(level):
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
        if level == "VERBOSE":
            from .. import VERBOSE
            return VERBOSE
        return getattr(logging, level, None)

    @staticmethod
    def _getLsstLogLevel(level):
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
        elif level == "VERBOSE":
            # LSST log does not yet have verbose defined
            return (lsstLog.Log.DEBUG + lsstLog.Log.INFO) // 2
        return getattr(lsstLog.Log, level, None)

    class ComponentSettings:
        """Container for log level values for a logging component."""
        def __init__(self, component):
            self.component = component
            self.pythonLogLevel = logging.getLogger(component).level
            self.lsstLogLevel = (lsstLog.Log.getLogger(component or "").getLevel()
                                 if lsstLog is not None else None)
            if self.lsstLogLevel == -1:
                self.lsstLogLevel = CliLog.defaultLsstLogLevel

        def __repr__(self):
            return (f"ComponentSettings(component={self.component}, pythonLogLevel={self.pythonLogLevel}, "
                    f"lsstLogLevel={self.lsstLogLevel})")

    @classmethod
    def _recordComponentSetting(cls, component):
        """Cache current levels for the given component in the list of
        component levels."""
        componentSettings = cls.ComponentSettings(component)
        cls._componentSettings.append(componentSettings)

    @classmethod
    def replayConfigState(cls, configState):
        """Re-create configuration using configuration state recorded earlier.

        Parameters
        ----------
        configState : `list` of `tuple`
            Tuples contain a method as first item and arguments for the method,
            in the same format as ``cls.configState``.
        """
        if cls._initialized or cls.configState:
            # Already initialized, do not touch anything.
            log = logging.getLogger(__name__)
            log.warning("Log is already initialized, will not replay configuration.")
            return

        # execute each one in order
        for call in configState:
            method, *args = call
            method(*args)
