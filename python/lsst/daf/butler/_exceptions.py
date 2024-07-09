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

"""Specialized Butler exceptions."""
__all__ = (
    "ButlerUserError",
    "CalibrationLookupError",
    "CollectionCycleError",
    "CollectionTypeError",
    "DataIdValueError",
    "DatasetNotFoundError",
    "DimensionNameError",
    "DatasetTypeNotSupportedError",
    "EmptyQueryResultError",
    "InconsistentDataIdError",
    "InvalidQueryError",
    "MissingDatasetTypeError",
    "MissingCollectionError",
    "ValidationError",
)

from ._exceptions_legacy import CollectionError, DataIdError, DatasetTypeError


class ButlerUserError(Exception):
    """Base class for Butler exceptions that contain a user-facing error
    message.

    Parameters
    ----------
    detail : `str`
        Details about the error that occurred.
    """

    # When used with Butler server, exceptions inheriting from
    # this class will be sent to the client side and re-raised by RemoteButler
    # there.  Be careful that error messages do not contain security-sensitive
    # information.
    #
    # This should only be used for "expected" errors that occur because of
    # errors in user-supplied data passed to Butler methods.  It should not be
    # used for any issues caused by the Butler configuration file, errors in
    # the library code itself or the underlying databases.
    #
    # When you create a new subclass of this type, add it to the list in
    # _USER_ERROR_TYPES below.

    error_type: str
    """Unique name for this error type, used to identify it when sending
    information about the error to the client.
    """

    def __init__(self, detail: str):
        return super().__init__(detail)


class CalibrationLookupError(LookupError, ButlerUserError):
    """Exception raised for failures to look up a calibration dataset.

    For a find-first query involving a calibration dataset to work, either the
    query's result rows need to include a temporal dimension or needs to be
    constrained temporally, such that each result row corresponds to a unique
    calibration dataset.  This exception can be raised if those dimensions or
    constraint are missing, or if a temporal dimension timespan overlaps
    multiple validity ranges (e.g. the recommended bias changes in the middle
    of an exposure).
    """

    error_type = "calibration_lookup"


class CollectionCycleError(ValueError, ButlerUserError):
    """Raised when an operation would cause a chained collection to be a child
    of itself.
    """

    error_type = "collection_cycle"


class CollectionTypeError(CollectionError, ButlerUserError):
    """Exception raised when type of a collection is incorrect."""

    error_type = "collection_type"


class DataIdValueError(DataIdError, ButlerUserError):
    """Exception raised when a value specified in a data ID does not exist."""

    error_type = "data_id_value"


class DatasetNotFoundError(LookupError, ButlerUserError):
    """The requested dataset could not be found."""

    error_type = "dataset_not_found"


class DimensionNameError(KeyError, DataIdError, ButlerUserError):
    """Exception raised when a dimension specified in a data ID does not exist
    or required dimension is not provided.
    """

    error_type = "dimension_name"


class DimensionValueError(ValueError, ButlerUserError):
    """Exception raised for issues with dimension values in a data ID."""

    error_type = "dimension_value"


class InconsistentDataIdError(DataIdError, ButlerUserError):
    """Exception raised when a data ID contains contradictory key-value pairs,
    according to dimension relationships.
    """

    error_type = "inconsistent_data_id"


class InvalidQueryError(ButlerUserError):
    """Exception raised when a query is not valid."""

    error_type = "invalid_query"


class MissingCollectionError(CollectionError, ButlerUserError):
    """Exception raised when an operation attempts to use a collection that
    does not exist.
    """

    error_type = "missing_collection"


class UnimplementedQueryError(NotImplementedError, ButlerUserError):
    """Exception raised when the query system does not support the query
    specified by the user.
    """

    error_type = "unimplemented_query"


class MissingDatasetTypeError(DatasetTypeError, KeyError, ButlerUserError):
    """Exception raised when a dataset type does not exist."""

    error_type = "missing_dataset_type"


class DatasetTypeNotSupportedError(RuntimeError):
    """A `DatasetType` is not handled by this routine.

    This can happen in a `Datastore` when a particular `DatasetType`
    has no formatters associated with it.
    """

    pass


class ValidationError(RuntimeError):
    """Some sort of validation error has occurred."""

    pass


class EmptyQueryResultError(Exception):
    """Exception raised when query methods return an empty result and `explain`
    flag is set.

    Parameters
    ----------
    reasons : `list` [`str`]
        List of possible reasons for an empty query result.
    """

    def __init__(self, reasons: list[str]):
        self.reasons = reasons

    def __str__(self) -> str:
        # There may be multiple reasons, format them into multiple lines.
        return "Possible reasons for empty result:\n" + "\n".join(self.reasons)


class UnknownButlerUserError(ButlerUserError):
    """Raised when the server sends an ``error_type`` for which we don't know
    the corresponding exception type.  (This may happen if an old version of
    the Butler client library connects to a new server).
    """

    error_type = "unknown"


_USER_ERROR_TYPES: tuple[type[ButlerUserError], ...] = (
    CalibrationLookupError,
    CollectionCycleError,
    CollectionTypeError,
    DimensionNameError,
    DimensionValueError,
    DataIdValueError,
    DatasetNotFoundError,
    InconsistentDataIdError,
    InvalidQueryError,
    MissingCollectionError,
    MissingDatasetTypeError,
    UnimplementedQueryError,
    UnknownButlerUserError,
)
_USER_ERROR_MAPPING = {e.error_type: e for e in _USER_ERROR_TYPES}
assert len(_USER_ERROR_MAPPING) == len(
    _USER_ERROR_TYPES
), "Subclasses of ButlerUserError must have unique 'error_type' property"


def create_butler_user_error(error_type: str, message: str) -> ButlerUserError:
    """Instantiate one of the subclasses of `ButlerUserError` based on its
    ``error_type`` string.

    Parameters
    ----------
    error_type : `str`
        The value from the ``error_type`` class attribute on the exception
        subclass you wish to instantiate.
    message : `str`
        Detailed error message passed to the exception constructor.
    """
    cls = _USER_ERROR_MAPPING.get(error_type)
    if cls is None:
        raise UnknownButlerUserError(f"Unknown exception type '{error_type}': {message}")
    return cls(message)
