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

from __future__ import annotations

__all__ = ("VERBOSE", "ButlerMDC", "ButlerLogRecords", "ButlerLogRecordHandler",
           "ButlerLogRecord", "JsonFormatter")

import logging
import datetime
import traceback
from typing import List, Union, Optional, ClassVar, Iterable, Iterator, Dict

from logging import LogRecord, StreamHandler, Formatter
from pydantic import BaseModel, ValidationError

VERBOSE = (logging.INFO + logging.DEBUG) // 2
"""Verbose log level"""

_LONG_LOG_FORMAT = "{levelname} {asctime} {name} {filename}:{lineno} - {message}"
"""Default format for log records."""

logging.addLevelName(VERBOSE, "VERBOSE")


class MDCDict(dict):
    """Dictionary for MDC data.

    This is internal class used for better formatting of MDC in Python logging
    output. It behaves like `defaultdict(str)` but overrides ``__str__`` and
    ``__repr__`` method to produce output better suited for logging records.
    """

    def __getitem__(self, name: str) -> str:
        """Return value for a given key or empty string for missing key.
        """
        return self.get(name, "")

    def __str__(self) -> str:
        """Return string representation, strings are interpolated without
        quotes.
        """
        items = (f"{k}={self[k]}" for k in sorted(self))
        return "{" + ", ".join(items) + "}"

    def __repr__(self) -> str:
        return str(self)


class ButlerMDC:
    """Handle setting and unsetting of global MDC records.

    The Mapped Diagnostic Context (MDC) can be used to set context
    for log messages.

    Currently there is one global MDC dict. Per-thread MDC is not
    yet supported.
    """

    _MDC = MDCDict()

    @classmethod
    def MDC(cls, key: str, value: str) -> str:
        """Set MDC for this key to the supplied value.

        Parameters
        ----------
        key : `str`
            Key to modify.
        value : `str`
            New value to use.

        Returns
        -------
        old : `str`
            The previous value for this key.
        """
        old_value = cls._MDC[key]
        cls._MDC[key] = value
        return old_value

    @classmethod
    def MDCRemove(cls, key: str) -> None:
        """Clear the MDC value associated with this key.

        Can be called even if the key is not known to MDC.
        """
        cls._MDC.pop(key, None)


class ButlerLogRecord(BaseModel):
    """A model representing a `logging.LogRecord`.

    A `~logging.LogRecord` always uses the current time in its record
    when recreated and that makes it impossible to use it as a
    serialization format. Instead have a local representation of a
    `~logging.LogRecord` that matches Butler needs.
    """

    _log_format: ClassVar[str] = _LONG_LOG_FORMAT

    name: str
    asctime: datetime.datetime
    message: str
    levelno: int
    levelname: str
    filename: str
    pathname: str
    lineno: int
    funcName: Optional[str]
    process: int
    processName: str
    exc_info: Optional[str]
    MDC: Dict[str, str]

    class Config:
        """Pydantic model configuration."""

        allow_mutation = False

    @classmethod
    def from_record(cls, record: LogRecord) -> ButlerLogRecord:
        """Create a new instance from a `~logging.LogRecord`.

        Parameters
        ----------
        record : `logging.LogRecord`
            The record from which to extract the relevant information.
        """
        # The properties that are one-to-one mapping.
        simple = ("name", "levelno", "levelname", "filename", "pathname",
                  "lineno", "funcName", "process", "processName")

        record_dict = {k: getattr(record, k) for k in simple}

        record_dict["message"] = record.getMessage()

        # MDC -- ensure the contents are copied to prevent any confusion
        # over the MDC global being updated later.
        record_dict["MDC"] = dict(getattr(record, "MDC", {}))

        # Always use UTC because in distributed systems we can't be sure
        # what timezone localtime is and it's easier to compare logs if
        # every system is using the same time.
        record_dict["asctime"] = datetime.datetime.fromtimestamp(record.created,
                                                                 tz=datetime.timezone.utc)

        # Sometimes exception information is included so must be
        # extracted.
        if record.exc_info:
            etype = record.exc_info[0]
            evalue = record.exc_info[1]
            tb = record.exc_info[2]
            record_dict["exc_info"] = "\n".join(traceback.format_exception(etype, evalue, tb))

        return cls(**record_dict)

    def format(self, log_format: Optional[str] = None) -> str:
        """Format this record.

        Parameters
        ----------
        log_format : `str`, optional
            The format string to use. This string follows the standard
            f-style use for formatting log messages. If `None`
            the class default will be used.

        Returns
        -------
        text : `str`
            The formatted log message.
        """
        if log_format is None:
            log_format = self._log_format

        as_dict = self.dict()

        as_dict["asctime"] = as_dict["asctime"].isoformat()
        formatted = log_format.format(**as_dict)
        return formatted

    def __str__(self) -> str:
        return self.format()


# The class below can convert LogRecord to ButlerLogRecord if needed.
Record = Union[LogRecord, ButlerLogRecord]


# Do not inherit from MutableSequence since mypy insists on the values
# being Any even though we wish to constrain them to Record.
class ButlerLogRecords(BaseModel):
    """Class representing a collection of `ButlerLogRecord`.
    """

    __root__: List[ButlerLogRecord]
    _log_format: Optional[str] = None

    @classmethod
    def from_records(cls, records: Iterable[ButlerLogRecord]) -> ButlerLogRecords:
        """Create collection from iterable.

        Parameters
        ----------
        records : iterable of `ButlerLogRecord`
            The records to seed this class with.
        """
        return cls(__root__=list(records))

    @property
    def log_format(self) -> str:
        if self._log_format is None:
            return _LONG_LOG_FORMAT
        return self._log_format

    @log_format.setter
    def log_format(self, format: str) -> None:
        self._log_format = format

    def __len__(self) -> int:
        return len(self.__root__)

    # The signature does not match the one in BaseModel but that is okay
    # if __root__ is being used.
    # See https://pydantic-docs.helpmanual.io/usage/models/#custom-root-types
    def __iter__(self) -> Iterator[ButlerLogRecord]:  # type: ignore
        return iter(self.__root__)

    def __setitem__(self, index: int, value: Record) -> None:
        self.__root__[index] = self._validate_record(value)

    def __getitem__(self, index: Union[slice, int]) -> Union[ButlerLogRecords, ButlerLogRecord]:
        # Handles slices and returns a new collection in that
        # case.
        item = self.__root__[index]
        if isinstance(item, list):
            return type(self)(__root__=item)
        else:
            return item

    def __reversed__(self) -> Iterator[ButlerLogRecord]:
        return self.__root__.__reversed__()

    def __delitem__(self, index: int) -> None:
        del self.__root__[index]

    def __str__(self) -> str:
        # Ensure that every record uses the same format string.
        return "\n".join(record.format(self.log_format) for record in self.__root__)

    def _validate_record(self, record: Record) -> ButlerLogRecord:
        if isinstance(record, ButlerLogRecord):
            pass
        elif isinstance(record, LogRecord):
            record = ButlerLogRecord.from_record(record)
        else:
            raise ValidationError(f"Can only append item of type {type(record)}")
        return record

    def insert(self, index: int, value: Record) -> None:
        self.__root__.insert(index, self._validate_record(value))

    def append(self, value: Record) -> None:
        value = self._validate_record(value)
        self.__root__.append(value)

    def clear(self) -> None:
        self.__root__.clear()

    def extend(self, records: Iterable[Record]) -> None:
        self.__root__.extend(self._validate_record(record) for record in records)

    def pop(self, index: int = -1) -> ButlerLogRecord:
        return self.__root__.pop(index)

    def reverse(self) -> None:
        self.__root__.reverse()


class ButlerLogRecordHandler(StreamHandler):
    """Python log handler that accumulates records.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records = ButlerLogRecords(__root__=[])

    def emit(self, record: LogRecord) -> None:
        self.records.append(record)


class JsonFormatter(Formatter):
    """Format a `LogRecord` in JSON format."""

    def format(self, record: LogRecord) -> str:
        butler_record = ButlerLogRecord.from_record(record)
        return butler_record.json()
