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

__all__ = ("PersistenceContextVars",)


import uuid
from collections.abc import Callable, Hashable
from contextvars import Context, ContextVar, Token, copy_context
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from ._dataset_ref import DatasetRef
    from ._dataset_type import DatasetType, SerializedDatasetType
    from .datastore.record_data import DatastoreRecordData
    from .dimensions._coordinate import DataCoordinate, SerializedDataCoordinate
    from .dimensions._records import DimensionRecord, SerializedDimensionRecord

_T = TypeVar("_T")
_V = TypeVar("_V")

_P = ParamSpec("_P")
_Q = ParamSpec("_Q")


class PersistenceContextVars:
    r"""Helper class for deserializing butler data structures.

    When serializing various butler data structures nested dataset types get
    serialized independently. This means what were multiple references to the
    same object in memory are all duplicated in the serialization process.

    Upon deserialization multiple independent data structures are created to
    represent the same logical bit of data.

    This class can be used to remove this duplication by caching objects as
    they are created and returning a reference to that object. This is done in
    concert with ``direct`` and ``from_simple`` methods on the various butler
    dataset structures.

    This class utilizes class level variables as a form of global state. Each
    of the various data structures can look to see if these global caches has
    been initialized as a cache (a dictionary) or is in the default None state.

    Users of this class are intended to create an instance, and then call the
    `run` method, supplying a callable function, and passing any required
    arguments. The `run` method then creates a specific execution context,
    initializing the caches, and then runs the supplied function. Upon
    completion of the function call, the caches are cleared and returned to the
    default state.

    This process is thread safe.

    Notes
    -----
    Caches of `SerializedDatasetRef`\ s are intentionally left out. It was
    discovered that these caused excessive python memory allocations which
    though cleaned up upon completion, left the process using more memory than
    it otherwise needed as python does not return allocated memory to the OS
    until process completion. It was determined the runtime cost of recreating
    the `SerializedDatasetRef`\ s was worth the memory savings.
    """

    serializedDatasetTypeMapping: ContextVar[dict[tuple[str, str], SerializedDatasetType] | None] = (
        ContextVar("serializedDatasetTypeMapping", default=None)
    )
    r"""A cache of `SerializedDatasetType`\ s.
    """

    serializedDataCoordinateMapping: ContextVar[
        dict[tuple[frozenset, bool], SerializedDataCoordinate] | None
    ] = ContextVar("serializedDataCoordinateMapping", default=None)
    r"""A cache of `SerializedDataCoordinate`\ s.
    """

    serializedDimensionRecordMapping: ContextVar[
        dict[tuple[str, frozenset] | tuple[int, DataCoordinate], SerializedDimensionRecord] | None
    ] = ContextVar("serializedDimensionRecordMapping", default=None)
    r"""A cache of `SerializedDimensionRecord`\ s.
    """

    loadedTypes: ContextVar[dict[tuple[str, str], DatasetType] | None] = ContextVar(
        "loadedTypes", default=None
    )
    r"""A cache of `DatasetType`\ s.
    """

    dataCoordinates: ContextVar[dict[tuple[frozenset, bool], DataCoordinate] | None] = ContextVar(
        "dataCoordinates", default=None
    )
    r"""A cache of `DataCoordinate`\ s.
    """

    datasetRefs: ContextVar[dict[int, DatasetRef] | None] = ContextVar("datasetRefs", default=None)
    r"""A cache of `DatasetRef`\ s.

    Keys are UUID converted to int, but only refs of parent dataset types are
    cached AND THE STORAGE CLASS IS UNSPECIFIED; consumers of this cache must
    call overrideStorageClass on the result.
    """

    dimensionRecords: ContextVar[dict[Hashable, DimensionRecord] | None] = ContextVar(
        "dimensionRecords", default=None
    )
    r"""A cache of `DimensionRecord`\ s.
    """

    dataStoreRecords: ContextVar[dict[frozenset[str | uuid.UUID], DatastoreRecordData] | None] = ContextVar(
        "dataStoreRecords", default=None
    )
    r"""A cache of `DatastoreRecordData` objects.
    """

    @classmethod
    def _getContextVars(cls) -> dict[str, ContextVar]:
        """Build a dictionary of names to caches declared at class scope."""
        classAttributes: dict[str, ContextVar] = {}
        for k in vars(cls):
            v = getattr(cls, k)
            # filter out callables and private attributes
            if not callable(v) and not k.startswith("__"):
                classAttributes[k] = v
        return classAttributes

    def __init__(self) -> None:
        self._ctx: Context | None = None
        self._tokens: dict[str, Token] | None = None

    def _functionRunner(self, function: Callable[_P, _V], *args: _P.args, **kwargs: _P.kwargs) -> _V:
        # create a storage space for the tokens returned from setting the
        # context variables
        self._tokens = {}

        # Set each cache to an empty dictionary and record the token returned
        # by this operation.
        for name, attribute in self._getContextVars().items():
            self._tokens[name] = attribute.set({})

        # Call the supplied function and record the result
        result = function(*args, **kwargs)

        # Reset all the context variables back to the state they were in before
        # this function was run.
        persistenceVars = self._getContextVars()
        assert self._tokens is not None
        for name, token in self._tokens.items():
            attribute = persistenceVars[name]
            attribute.reset(token)
        self._tokens = None
        return result

    def run(self, function: Callable[_Q, _T], *args: _Q.args, **kwargs: _Q.kwargs) -> _T:
        """Execute the supplied function inside context specific caches.

        Parameters
        ----------
        function : `Callable`
            A callable which is to be executed inside a specific context.
        *args : tuple
            Positional arguments which are to be passed to the `Callable`.
        **kwargs : dict, optional
            Extra key word arguments which are to be passed to the `Callable`.

        Returns
        -------
        result : `Any`
            The result returned by executing the supplied `Callable`.
        """
        self._ctx = copy_context()
        # Type checkers seem to have trouble with a second layer nesting of
        # parameter specs in callables, so ignore the call here and explicitly
        # cast the result as we know this is exactly what the return type will
        # be.
        result = self._ctx.run(self._functionRunner, function, *args, **kwargs)  # type: ignore
        return result
