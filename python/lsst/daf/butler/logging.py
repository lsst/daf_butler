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

__all__ = (
    "ButlerLogRecord",
    "ButlerLogRecordHandler",
    "ButlerLogRecords",
    "ButlerMDC",
    "JsonLogFormatter",
)

import datetime
import io
import logging
import traceback
from collections.abc import Callable, Generator, Iterable, Iterator, MutableSequence
from contextlib import contextmanager
from logging import Formatter, LogRecord, StreamHandler
from types import TracebackType
from typing import IO, Any, ClassVar, Literal, Self, TypeVar, cast, overload

import pydantic_core
from pydantic import BaseModel, ConfigDict, Field, RootModel

_LONG_LOG_FORMAT = "{levelname} {asctime} {name} {filename}:{lineno} - {message}"
"""Default format for log records."""


class MDCDict(dict):
    """Dictionary for MDC data.

    This is internal class used for better formatting of MDC in Python logging
    output. It behaves like `defaultdict(str)` but overrides ``__str__`` and
    ``__repr__`` method to produce output better suited for logging records.
    """

    def __getitem__(self, name: str) -> str:
        """Return value for a given key or empty string for missing key."""
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

    _old_factory: Callable[..., logging.LogRecord] | None = None
    """Old log record factory."""

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

        Parameters
        ----------
        key : `str`
            Key for which the MDC value should be removed.
        """
        cls._MDC.pop(key, None)

    @classmethod
    def clear_mdc(cls) -> None:
        """Clear all MDC entries."""
        cls._MDC.clear()

    @classmethod
    @contextmanager
    def set_mdc(cls, mdc: dict[str, str]) -> Generator[None, None, None]:
        """Set the MDC key for this context.

        This context manager also adds a mapping named ``mdc`` to any
        exceptions that escape it.

        Parameters
        ----------
        mdc : `dict` of `str`, `str`
            MDC keys to update temporarily.

        Notes
        -----
        Other MDC keys are not modified. The previous values are restored
        on exit (removing them if the were unset previously).
        """
        previous = {}
        for k, v in mdc.items():
            previous[k] = cls.MDC(k, v)

        try:
            yield
        except BaseException as e:
            # In logging, inner context overrules outer context. Need the same
            # for exceptions.
            inner_context: MDCDict = e.mdc if hasattr(e, "mdc") else MDCDict()
            e.mdc = cls._MDC.copy() | inner_context  # type: ignore[attr-defined]
            raise
        finally:
            for k, v in previous.items():
                if not v:
                    cls.MDCRemove(k)
                else:
                    cls.MDC(k, v)

    @classmethod
    def add_mdc_log_record_factory(cls) -> None:
        """Add a log record factory that adds a MDC record to `LogRecord`."""
        old_factory = logging.getLogRecordFactory()

        # Need explicit args in case exc_info is called as an arg (and because
        # the factory API has a parameter literally called `args`).
        def record_factory(
            name: str,
            level: int,
            fn: str,
            lno: int,
            msg: str,
            args: tuple,
            exc_info: tuple | None | Literal[False],
            func: str | None = None,
            sinfo: TracebackType | None = None,
            **kwargs: Any,
        ) -> LogRecord:
            record = old_factory(name, level, fn, lno, msg, args, exc_info, func, sinfo, **kwargs)
            # Make sure we send a copy of the global dict in the record.
            mdc = MDCDict(cls._MDC)
            if exc_info is not None and exc_info is not False:
                _, ex, _ = exc_info
                # TODO: this doesn't handle chained exceptions, fix on DM-47546
                if hasattr(ex, "mdc"):
                    # Context at the point where the exception was raised
                    # takes precedence.
                    mdc.update(ex.mdc)
            record.MDC = mdc
            return record

        cls._old_factory = old_factory
        logging.setLogRecordFactory(record_factory)

    @classmethod
    def restore_log_record_factory(cls) -> None:
        """Restores the log record factory to the original form.

        Does nothing if there has not been a call to
        `add_mdc_log_record_factory`.
        """
        if cls._old_factory:
            logging.setLogRecordFactory(cls._old_factory)


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
    funcName: str | None = None
    process: int
    processName: str
    exc_info: str | None = None
    MDC: dict[str, str]

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_record(cls, record: LogRecord) -> ButlerLogRecord:
        """Create a new instance from a `~logging.LogRecord`.

        Parameters
        ----------
        record : `logging.LogRecord`
            The record from which to extract the relevant information.
        """
        # The properties that are one-to-one mapping.
        simple = (
            "name",
            "levelno",
            "levelname",
            "filename",
            "pathname",
            "lineno",
            "funcName",
            "process",
            "processName",
        )

        record_dict = {k: getattr(record, k) for k in simple}

        record_dict["message"] = record.getMessage()

        # MDC -- ensure the contents are copied to prevent any confusion
        # over the MDC global being updated later.
        record_dict["MDC"] = dict(getattr(record, "MDC", {}))

        # Always use UTC because in distributed systems we can't be sure
        # what timezone localtime is and it's easier to compare logs if
        # every system is using the same time.
        record_dict["asctime"] = datetime.datetime.fromtimestamp(record.created, tz=datetime.UTC)

        # Sometimes exception information is included so must be
        # extracted.
        if record.exc_info:
            etype = record.exc_info[0]
            evalue = record.exc_info[1]
            tb = record.exc_info[2]
            record_dict["exc_info"] = "\n".join(traceback.format_exception(etype, evalue, tb))

        return cls(**record_dict)

    def format(self, log_format: str | None = None) -> str:
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

        as_dict = self.model_dump()

        # Special case MDC content. Convert it to an MDCDict
        # so that missing items do not break formatting.
        as_dict["MDC"] = MDCDict(as_dict["MDC"])

        as_dict["asctime"] = as_dict["asctime"].isoformat()
        formatted = log_format.format(**as_dict)
        return formatted

    def __str__(self) -> str:
        return self.format()


# The class below can convert LogRecord to ButlerLogRecord if needed.
Record = LogRecord | ButlerLogRecord


_T = TypeVar("_T", bound="ButlerLogRecords")


class _ButlerLogRecordsModelV1(RootModel):
    """Pydantic model used for version 1 JSON log files: a simple list of
    `ButlerLogRecord` instances.
    """

    root: list[ButlerLogRecord]

    def wrap(self, container_type: type[_T]) -> _T:
        """Convert this model into a `ButlerLogRecords` instance.

        Parameters
        ----------
        container_type : `type`
            `ButlerLogRecords` subclass to use.

        Returns
        -------
        container : `ButlerLogRecords`
            Container for log records.
        """
        return container_type.from_records(self.root)


class _ButlerLogRecordsModel(BaseModel):
    """Pydantic model used for version 2(+) JSON log files: a struct with a
    `records` attribute.
    """

    CURRENT_VERSION: ClassVar[int] = 2
    """File format version written by this module."""

    extended: Literal[True]
    """Constant field that appears at the beginning of every version 2+ JSON
    log file.

    This field must be initialized directly when the model is constructed,
    because using a default causes it to be omitted by Pydantic when
    serializing with ``exclude_unset`` or ``exclude_default`` (both of which
    are used in serializing logs).
    """

    version: int
    """File format version for this file.

    This field must be initialized directly when the model is constructed,
    because using a default causes it to be omitted by Pydantic when
    serializing with ``exclude_unset`` or ``exclude_default`` (both of which
    are used in serializing logs).
    """

    records: list[ButlerLogRecord] = Field(default_factory=list)
    """The actual log records."""

    model_config = ConfigDict(extra="allow")

    def wrap(self, container_type: type[_T]) -> _T:
        """Convert this model into a `ButlerLogRecords` instance.

        Parameters
        ----------
        container_type : `type`
            `ButlerLogRecords` subclass to use.

        Returns
        -------
        container : `ButlerLogRecords`
            Container for log records.
        """
        return container_type.from_records(self.records, extra=self.__pydantic_extra__)


class ButlerLogRecords(MutableSequence[ButlerLogRecord]):
    """A container class for `ButlerLogRecord` objects.

    Parameters
    ----------
    records : `list` [`ButlerLogRecord`]
        List of records to use directly as the backing store for the container.
    extra : `dict`, optional
        Additional JSON data included with the log records.  Subclasses may
        interpret structured information, but the base class just sets this as
        the `extra` attribute.

    Notes
    -----
    ButlerLogRecords supports two different file formats:

    - Full-container serialization to JSON, in which records are stored in a
      ``records`` array and extra fields may be present (for backwards
      compatibility, *reading* records directly from a JSON list is also
      supported).

    - Streaming serialization, in which each each line is a one-line JSON
      representation of a single `ButlerLogRecord`.  Extra fields may be added
      included to this format by appending a single line with the value given
      by `STREAMING_EXTRA_DELIMITER`, which indicates that the remainder of the
      file is a single JSON block.

    Subclasses of `ButlerLogRecords` are expected to support these formats by
    using the "extra" JSON fields to hold any additional state.   If subclasses
    intercept `extra` at construction in a way that prevents that information
    from being held in the base class `extra` field, they must override
    `_from_record_subset` and `to_json_data` to pass that state to slices and
    save it, respectively.
    """

    def __init__(self, records: list[ButlerLogRecord], extra: dict[str, object] | None = None):
        self._records = records
        self._log_format: str | None = None
        self.extra = extra if extra is not None else {}

    STREAMING_EXTRA_DELIMITER: ClassVar[str] = "###EXTRA###"
    """Special string written (on its own line) after streamed log records to
    indicate that the rest of the file is a JSON blob of "extra" data.
    """

    @classmethod
    def from_records(cls, records: Iterable[ButlerLogRecord], extra: dict[str, object] | None = None) -> Self:
        """Create collection from iterable.

        Parameters
        ----------
        records : iterable of `ButlerLogRecord` or `LogRecord`
            The records to seed this class with.
        extra : `dict`, optional
            Additional JSON data included with the log records.  Subclasses may
            interpret structured information, but the base class just sets this
            as the `extra` attribute.

        Returns
        -------
        container : `ButlerLogRecords`
            New log record container.
        """
        return cls(list(records), extra=extra)

    @classmethod
    def from_file(cls, filename: str) -> Self:
        """Read records from file.

        Parameters
        ----------
        filename : `str`
            Name of file containing the JSON records.

        Notes
        -----
        Works with one-record-per-line format JSON files and a direct
        serialization of the Pydantic model.
        """
        with open(filename) as fd:
            return cls.from_stream(fd)

    def _from_record_subset(self, records: list[ButlerLogRecord]) -> Self:
        """Return a new instance with a subset of the original's records.

        Parameters
        ----------
        records : `list` [`ButlerLogRecord`]
            New list of records.

        Returns
        -------
        copy : `ButlerLogRecords`
            New instance of this type.

        Notes
        -----
        This is a hook provided to allow subclasses to transfer extra state
        when the container is sliced.
        """
        return self.from_records(records, extra=self.extra)

    @staticmethod
    def _generic_startswith(startdata: str | bytes, prefix: str) -> bool:
        # A type-safe wrapper for `str.startswith` and `bytes.startswith` that
        # avoids converting 'startdata', as it might be big.
        if isinstance(startdata, str):
            return startdata.startswith(prefix)
        else:
            return startdata.startswith(prefix.encode())

    @classmethod
    def _detect_model(
        cls,
        startdata: str | bytes,
    ) -> type[_ButlerLogRecordsModelV1] | type[_ButlerLogRecordsModel] | None:
        """Given some representative data, determine if this is a serialized
        model or a streaming format.

        Parameters
        ----------
        startdata : `bytes` or `str`
            Representative characters or bytes from the start of a serialized
            collection of log records.

        Returns
        -------
        model_type : `type` or `None`
            If the data looks like either the `_ButlerLogRecords` or
            `_ExtendedButlerLogRecords` models, that model type.
            Otherwise (streaming records, or an empty string `None`).

        Raises
        ------
        ValueError
            Raised if the sentinel doesn't look like either of the supported
            log record formats.
        """
        if not startdata:
            return None

        if cls._generic_startswith(startdata, "["):
            # This is a JSON array of records.
            return _ButlerLogRecordsModelV1
        elif cls._generic_startswith(startdata, cls.STREAMING_EXTRA_DELIMITER):
            # This is an empty log file with a log record per line format.
            return None
        elif not cls._generic_startswith(startdata, "{"):
            # Limit the length of string reported in error message in case
            # this is an enormous file.
            max = 32
            if len(startdata) > max:
                msg = f"{startdata[:max]!r}..."
            else:
                msg = repr(startdata)
            raise ValueError(
                f"Unrecognized JSON log format. Expected '{{' or '[' but got {startdata[0]!r} from {msg}."
            )
        elif cls._generic_startswith(startdata, '{"extended":true'):
            # This is a JSON model with a field for records.
            return _ButlerLogRecordsModel
        # Assume a record per line.
        return None

    @classmethod
    def from_stream(cls, stream: IO) -> Self:
        """Read records from I/O stream.

        Parameters
        ----------
        stream : `typing.IO`
            Stream from which to read JSON records.

        Returns
        -------
        container : `ButlerLogRecords`
            New log record container.
        """
        first_line = stream.readline()

        empty_stream = False
        if not first_line:
            # Empty file, return zero records.
            return cls.from_records([])
        elif cls._generic_startswith(first_line, cls.STREAMING_EXTRA_DELIMITER):
            empty_stream = True

        model_type = cls._detect_model(first_line)

        if model_type:
            # This is a ButlerLogRecords model serialization so all the
            # content must be read first.
            all = first_line + stream.read()
            return model_type.model_validate_json(all).wrap(cls)

        # A stream of records with one record per line.
        if not empty_stream:
            records = [ButlerLogRecord.model_validate_json(first_line)]
            for line in stream:
                line = line.rstrip()
                if cls._generic_startswith(line, cls.STREAMING_EXTRA_DELIMITER):
                    break
                elif line:  # skip blank lines
                    records.append(ButlerLogRecord.model_validate_json(line))
        else:
            # No records but might have extra metadata.
            records = []
        extra_data = stream.read()
        if extra_data:
            extra = pydantic_core.from_json(extra_data)
        else:
            extra = {}
        return cls.from_records(records, extra=extra)

    @classmethod
    def from_raw(cls, serialized: str | bytes) -> ButlerLogRecords:
        """Parse raw serialized form and return records.

        Parameters
        ----------
        serialized : `bytes` or `str`
            Either the serialized JSON of the model created using
            `write` or a streaming format of one JSON `ButlerLogRecord` per
            line. This can also support a zero-length string.

        Returns
        -------
        container : `ButlerLogRecords`
            New log record container.
        """
        if isinstance(serialized, str):
            return cls.from_stream(io.StringIO(serialized))
        else:
            return cls.from_stream(io.BytesIO(serialized))

    def to_json_data(self) -> str:
        """Serialize to a JSON string.

        Returns
        -------
        data : `str`
            String containing JSON data.
        """
        if self.extra:
            # There's no way to tell Pydantic to add extra fields back in
            # when serializing directly to JSON; we have to convert to a dict
            # first and then to JSON.
            py_data = _ButlerLogRecordsModel(
                extended=True, version=_ButlerLogRecordsModel.CURRENT_VERSION, records=self._records
            ).model_dump(exclude_unset=True, exclude_defaults=True)
            py_data.update(self.extra)
            return pydantic_core.to_json(py_data).decode()
        else:
            return _ButlerLogRecordsModel(
                extended=True, version=_ButlerLogRecordsModel.CURRENT_VERSION, records=self._records
            ).model_dump_json(exclude_unset=True, exclude_defaults=True)

    @property
    def log_format(self) -> str:
        """The log format string for these records."""
        if self._log_format is None:
            return _LONG_LOG_FORMAT
        return self._log_format

    @log_format.setter
    def log_format(self, format: str | None) -> None:
        self.set_log_format(format)

    def set_log_format(self, format: str | None) -> str | None:
        """Set the log format string for these records.

        This may also be set via the property; method is provided for
        backwards compatibility.

        Parameters
        ----------
        format : `str`, optional
            The new format string to use for converting this collection
            of records into a string. If `None` the default format will be
            used.

        Returns
        -------
        old_format : `str`, optional
            The previous log format.
        """
        previous = self._log_format
        self._log_format = format
        return previous

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Iterator[ButlerLogRecord]:
        return iter(self._records)

    @overload
    def __setitem__(self, index: int, value: Record) -> None: ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[Record]) -> None: ...

    def __setitem__(self, index: slice | int, value: Record | Iterable[Record]) -> None:
        if isinstance(index, slice):
            self._records[index] = [self._validate_record(v) for v in cast(Iterable[Record], value)]
        else:
            self._records[index] = self._validate_record(cast(Record, value))

    @overload
    def __getitem__(self, index: int) -> ButlerLogRecord: ...

    @overload
    def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(self, index: slice | int) -> Self | ButlerLogRecord:
        # Handles slices and returns a new collection in that
        # case.
        item = self._records[index]
        if isinstance(item, list):
            return self._from_record_subset(item)
        else:
            return item

    def __delitem__(self, index: slice | int) -> None:
        del self._records[index]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ButlerLogRecords):
            return self._records == other._records and self.extra == other.extra
        return NotImplemented

    def __str__(self) -> str:
        # Ensure that every record uses the same format string.
        return "\n".join(record.format(self.log_format) for record in self)

    def _validate_record(self, record: Record) -> ButlerLogRecord:
        if isinstance(record, ButlerLogRecord):
            pass
        elif isinstance(record, LogRecord):
            record = ButlerLogRecord.from_record(record)
        else:
            raise ValueError(f"Can only append item of type {type(record)}")
        return record

    def insert(self, index: int, value: Record) -> None:
        # Docstring provided by ABC.
        self._records.insert(index, self._validate_record(value))

    def append(self, value: Record) -> None:
        # Docstring provided by ABC.
        # Only overridden to accept `LogRecord`, not just `ButlerLogRecord`.
        self._records.append(self._validate_record(value))

    @classmethod
    def write_streaming_extra(cls, file: IO[str], extra_data: str) -> None:
        """Append the special delimiter and extra JSON data to a file written
        in streaming mode.

        Parameters
        ----------
        file : `typing.IO`
            File object to write to, pointing to the end of the streamed log
            records.
        extra_data : `str`
            Extra JSON data as a string.
        """
        print(cls.STREAMING_EXTRA_DELIMITER, file=file)
        file.write(extra_data)


class ButlerLogRecordHandler(StreamHandler):
    """Python log handler that accumulates records."""

    def __init__(self) -> None:
        super().__init__()
        self.records = ButlerLogRecords([])

    def emit(self, record: LogRecord) -> None:
        self.records.append(record)


class JsonLogFormatter(Formatter):
    """Format a `LogRecord` in JSON format."""

    def format(self, record: LogRecord) -> str:
        butler_record = ButlerLogRecord.from_record(record)
        return butler_record.model_dump_json(exclude_unset=True, exclude_defaults=True)
