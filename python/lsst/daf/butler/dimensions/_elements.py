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
    "Dimension",
    "DimensionCombination",
    "DimensionElement",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, cast

from lsst.utils.classes import cached_getter

from .. import ddl
from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalRelationshipEndpoint
from ..json import from_json_generic, to_json_generic

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from ..registry import Registry
    from ._governor import GovernorDimension
    from ._graph import DimensionGraph
    from ._group import DimensionGroup
    from ._records import DimensionRecord
    from ._universe import DimensionUniverse


class DimensionElement(TopologicalRelationshipEndpoint):
    """A label and/or metadata in the dimensions system.

    A named data-organization concept that defines a label and/or metadata
    in the dimensions system.

    A `DimensionElement` instance typically corresponds to a _logical_ table in
    the `Registry`: either an actual database table or a way of generating rows
    on-the-fly that can similarly participate in queries.  The rows in that
    table are represented by instances of a `DimensionRecord` subclass.  Most
    `DimensionElement` instances are instances of its `Dimension` subclass,
    which is used for elements that can be used as data ID keys.

    Notes
    -----
    `DimensionElement` instances should always be constructed by and retrieved
    from a `DimensionUniverse`.  They are immutable after they are fully
    constructed, and should never be copied.

    Pickling a `DimensionElement` just records its name and universe;
    unpickling one actually just looks up the element via the singleton
    dictionary of all universes.  This allows pickle to be used to transfer
    elements between processes, but only when each process initializes its own
    instance of the same `DimensionUniverse`.
    """

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name})"

    def __eq__(self, other: Any) -> bool:
        try:
            return self.name == other.name
        except AttributeError:
            # TODO: try removing this fallback; it's not really consistent with
            # base class intent, and it could be confusing
            return self.name == other

    def __hash__(self) -> int:
        return hash(self.name)

    # TODO: try removing comparison operators; DimensionUniverse.sorted should
    # be adequate.

    def __lt__(self, other: DimensionElement) -> bool:
        try:
            return self.universe.getElementIndex(self.name) < self.universe.getElementIndex(other.name)
        except KeyError:
            return NotImplemented

    def __le__(self, other: DimensionElement) -> bool:
        try:
            return self.universe.getElementIndex(self.name) <= self.universe.getElementIndex(other.name)
        except KeyError:
            return NotImplemented

    def __gt__(self, other: DimensionElement) -> bool:
        try:
            return self.universe.getElementIndex(self.name) > self.universe.getElementIndex(other.name)
        except KeyError:
            return NotImplemented

    def __ge__(self, other: DimensionElement) -> bool:
        try:
            return self.universe.getElementIndex(self.name) >= self.universe.getElementIndex(other.name)
        except KeyError:
            return NotImplemented

    @classmethod
    def _unpickle(cls, universe: DimensionUniverse, name: str) -> DimensionElement:
        """Callable used for unpickling.

        For internal use only.
        """
        return universe[name]

    def __reduce__(self) -> tuple:
        return (self._unpickle, (self.universe, self.name))

    def __deepcopy__(self, memo: dict) -> DimensionElement:
        # DimensionElement is recursively immutable; see note in @immutable
        # decorator.
        return self

    def to_simple(self, minimal: bool = False) -> str:
        """Convert this class to a simple python type.

        This is suitable for serialization.

        Parameters
        ----------
        minimal : `bool`, optional
            Use minimal serialization. Has no effect on for this class.

        Returns
        -------
        simple : `str`
            The object converted to a single string.
        """
        return self.name

    @classmethod
    def from_simple(
        cls, simple: str, universe: DimensionUniverse | None = None, registry: Registry | None = None
    ) -> DimensionElement:
        """Construct a new object from the simplified form.

        Usually the data is returned from the `to_simple` method.

        Parameters
        ----------
        simple : `str`
            The value returned by `to_simple()`.
        universe : `DimensionUniverse`
            The special graph of all known dimensions.
        registry : `lsst.daf.butler.Registry`, optional
            Registry from which a universe can be extracted. Can be `None`
            if universe is provided explicitly.

        Returns
        -------
        dataId : `DimensionElement`
            Newly-constructed object.
        """
        if universe is None and registry is None:
            raise ValueError("One of universe or registry is required to convert a dict to a DataCoordinate")
        if universe is None and registry is not None:
            universe = registry.dimensions
        if universe is None:
            # this is for mypy
            raise ValueError("Unable to determine a usable universe")

        return universe[simple]

    to_json = to_json_generic
    from_json: ClassVar = classmethod(from_json_generic)

    def hasTable(self) -> bool:
        """Indicate if this element is associated with a table.

        Return `True` if this element is associated with a table
        (even if that table "belongs" to another element).
        """
        return True

    universe: DimensionUniverse
    """The universe of all compatible dimensions with which this element is
    associated (`DimensionUniverse`).
    """

    @property
    @cached_getter
    def governor(self) -> GovernorDimension | None:
        """Return the governor dimension.

        This is the `GovernorDimension` that is a required dependency of this
        element, or `None` if there is no such dimension (`GovernorDimension`
        or `None`).
        """
        if len(self.minimal_group.governors) == 1:
            (result,) = self.minimal_group.governors
            return cast("GovernorDimension", self.universe[result])
        elif len(self.minimal_group.governors) > 1:
            raise RuntimeError(
                f"Dimension element {self.name} has multiple governors: {self.minimal_group.governors}."
            )
        else:
            return None

    @property
    @abstractmethod
    def required(self) -> NamedValueAbstractSet[Dimension]:
        """Return the required dimensions.

        Dimensions that are necessary to uniquely identify a record of this
        dimension element.

        For elements with a database representation, these dimension are
        exactly those used to form the (possibly compound) primary key, and all
        dimensions here that are not ``self`` are also used to form foreign
        keys.

        For `Dimension` instances, this should be exactly the same as
        ``graph.required``, but that may not be true for `DimensionElement`
        instances in general.  When they differ, there are multiple
        combinations of dimensions that uniquely identify this element, but
        this one is more direct.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        """Return the implied dimensions.

        Other dimensions that are uniquely identified directly by a record
        of this dimension element.

        For elements with a database representation, these are exactly the
        dimensions used to form foreign key constraints whose fields are not
        (wholly) also part of the primary key.

        Unlike ``self.graph.implied``, this set is not expanded recursively.
        """
        raise NotImplementedError()

    @property
    @cached_getter
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        """Return all dimensions.

        The union of `required` and `implied`, with all elements in
        `required` before any elements in `implied`.

        This differs from ``self.graph.dimensions`` both in order and in
        content:

        - as in ``self.implied``, implied dimensions are not expanded
          recursively here;
        - implied dimensions appear after required dimensions here, instead of
          being topologically ordered.

        As a result, this set is ordered consistently with
        ``self.RecordClass.fields``.
        """
        return NamedValueSet(list(self.required) + list(self.implied)).freeze()

    # Deprecated via a warning from its implementation.
    # TODO: remove on DM-41326.
    @property
    def graph(self) -> DimensionGraph:
        """Return minimal graph that includes this element (`DimensionGraph`).

        ``self.graph.required`` includes all dimensions whose primary key
        values are sufficient (often necessary) to uniquely identify ``self``
        (including ``self`` if ``isinstance(self, Dimension)``.
        ``self.graph.implied`` includes all dimensions also identified
        (possibly recursively) by this set.
        """
        return self.minimal_group._as_graph()

    @property
    @cached_getter
    def minimal_group(self) -> DimensionGroup:
        """Return minimal dimension group that includes this element.

        ``self.minimal_group.required`` includes all dimensions whose primary
        key values are sufficient (often necessary) to uniquely identify
        ``self`` (including ``self`` if ``isinstance(self, Dimension)``.
        ``self.minimal_group.implied`` includes all dimensions also identified
        (possibly recursively) by this set.
        """
        return self.universe.conform(self.dimensions.names)

    @property
    @cached_getter
    def RecordClass(self) -> type[DimensionRecord]:
        """Return the record subclass for this element.

        The `DimensionRecord` subclass used to hold records for this element
        (`type`).

        Because `DimensionRecord` subclasses are generated dynamically, this
        type cannot be imported directly and hence can only be obtained from
        this attribute.
        """
        from ._records import _subclassDimensionRecord

        return _subclassDimensionRecord(self)

    @property
    @abstractmethod
    def metadata(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        """Additional metadata fields included in this element's table.

        (`NamedValueSet` of `FieldSpec`).
        """
        raise NotImplementedError()

    @property
    def viewOf(self) -> str | None:
        """Name of another table this element's records are drawn from.

        (`str` or `None`).
        """
        return None

    @property
    def alwaysJoin(self) -> bool:
        """Indicate if the element should always be included.

        If `True`, always include this element in any query or data ID in
        which its ``required`` dimensions appear, because it defines a
        relationship between those dimensions that must always be satisfied.
        """
        return False

    @property
    @abstractmethod
    def populated_by(self) -> Dimension | None:
        """The dimension that this element's records are always inserted,
        exported, and imported alongside.

        Notes
        -----
        When this is `None` (as it will be, at least at first, for any data
        repositories created before this attribute was added), records for
        this element will often need to be exported manually when datasets
        associated with some other related dimension are exported, in order for
        the post-import data repository to function as expected.
        """
        raise NotImplementedError()


class Dimension(DimensionElement):
    """A dimension.

    A named data-organization concept that can be used as a key in a data
    ID.
    """

    @property
    @abstractmethod
    def uniqueKeys(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        """Return the unique fields.

        All fields that can individually be used to identify records of this
        element, given the primary keys of all required dependencies
        (`NamedValueAbstractSet` of `FieldSpec`).
        """
        raise NotImplementedError()

    @property
    @cached_getter
    def primaryKey(self) -> ddl.FieldSpec:
        """Return primary key field for this dimension (`FieldSpec`).

        Note that the database primary keys for dimension tables are in general
        compound; this field is the only field in the database primary key that
        is not also a foreign key (to a required dependency dimension table).
        """
        primaryKey, *_ = self.uniqueKeys
        return primaryKey

    @property
    @cached_getter
    def alternateKeys(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        """Return alternate keys.

        Additional unique key fields for this dimension that are not the
        primary key (`NamedValueAbstractSet` of `FieldSpec`).

        If this dimension has required dependencies, the keys of those
        dimensions are also included in the unique constraints defined for
        these alternate keys.
        """
        _, *alternateKeys = self.uniqueKeys
        return NamedValueSet(alternateKeys).freeze()

    @property
    def populated_by(self) -> Dimension:
        # Docstring inherited.
        return self


class DimensionCombination(DimensionElement):
    """Element with extra information.

    A `DimensionElement` that provides extra metadata and/or relationship
    endpoint information for a combination of dimensions.
    """
