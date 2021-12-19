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

__all__ = ("GovernorDimension",)

from types import MappingProxyType
from typing import TYPE_CHECKING, AbstractSet, Iterable, Mapping, Optional

from lsst.utils import doImportType

from .. import ddl
from .._topology import TopologicalFamily, TopologicalSpace
from ..named import NamedValueAbstractSet, NamedValueSet
from ._elements import Dimension
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

if TYPE_CHECKING:
    from ...registry.interfaces import Database, GovernorDimensionRecordStorage, StaticTablesContext


class GovernorDimension(Dimension):
    """Governor dimension.

    A special `Dimension` with no dependencies and a small number of rows,
    used to group the dimensions that depend on it.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    storage : `dict`
        Fully qualified name of the `GovernorDimensionRecordStorage` subclass
        that will back this element in the registry (in a "cls" key) along
        with any other construction keyword arguments (in other keys).
    metadata : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
        Field specifications for all non-key fields in this dimension's table.
    uniqueKeys : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.

    Notes
    -----
    Most dimensions have exactly one governor dimension as a required
    dependency, and queries that involve those dimensions are always expected
    to explicitly identify the governor dimension value(s), rather than
    retrieve all matches from the database.  Because governor values are thus
    almost always known at query-generation time, they can be used there to
    simplify queries, provide sensible defaults, or check in advance for common
    mistakes that might otherwise yield confusing (albeit formally correct)
    results instead of straightforward error messages.

    Governor dimensions may not be associated with any kind of topological
    extent.

    Governor dimension rows are often affiliated with a Python class or
    instance (e.g. `lsst.obs.base.Instrument`) that is capable of generating
    the rows of at least some dependent dimensions or providing other related
    functionality.  In the future, we hope to attach these instances to
    governor dimension records (instantiating them from information in the
    database row when it is fetched), and use those objects to add additional
    functionality to governor dimensions, but a number of (code) dependency
    relationships would need to be reordered first.
    """

    def __init__(
        self,
        name: str,
        storage: dict,
        *,
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
        uniqueKeys: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        self._name = name
        self._storage = storage
        self._required = NamedValueSet({self}).freeze()
        self._metadata = metadata
        self._uniqueKeys = uniqueKeys
        if self.primaryKey.getPythonType() is not str:
            raise TypeError(
                f"Governor dimension '{name}' must have a string primary key (configured type "
                f"is {self.primaryKey.dtype.__name__})."
            )
        if self.primaryKey.length is not None and self.primaryKey.length > self.MAX_KEY_LENGTH:
            raise TypeError(
                f"Governor dimension '{name}' must have a string primary key with length <= "
                f"{self.MAX_KEY_LENGTH} (configured value is {self.primaryKey.length})."
            )

    MAX_KEY_LENGTH = 128

    @property
    def name(self) -> str:
        # Docstring inherited from TopologicalRelationshipEndpoint.
        return self._name

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._required

    @property
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet().freeze()

    @property
    def topology(self) -> Mapping[TopologicalSpace, TopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return MappingProxyType({})

    @property
    def metadata(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from DimensionElement.
        return self._metadata

    @property
    def uniqueKeys(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from Dimension.
        return self._uniqueKeys

    def makeStorage(
        self,
        db: Database,
        *,
        context: Optional[StaticTablesContext] = None,
    ) -> GovernorDimensionRecordStorage:
        """Make storage record.

        Constructs the `DimensionRecordStorage` instance that should
        be used to back this element in a registry.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.

        Returns
        -------
        storage : `GovernorDimensionRecordStorage`
            Storage object that should back this element in a registry.
        """
        from ...registry.interfaces import GovernorDimensionRecordStorage

        cls = doImportType(self._storage["cls"])
        assert issubclass(cls, GovernorDimensionRecordStorage)
        return cls.initialize(db, self, context=context, config=self._storage)


class GovernorDimensionConstructionVisitor(DimensionConstructionVisitor):
    """A construction visitor for `GovernorDimension`.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    storage : `dict`
        Fully qualified name of the `GovernorDimensionRecordStorage` subclass
        that will back this element in the registry (in a "cls" key) along
        with any other construction keyword arguments (in other keys).
    metadata : `Iterable` [ `ddl.FieldSpec` ]
        Field specifications for all non-key fields in this element's table.
    uniqueKeys : `Iterable` [ `ddl.FieldSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.
    """

    def __init__(
        self,
        name: str,
        storage: dict,
        *,
        metadata: Iterable[ddl.FieldSpec] = (),
        uniqueKeys: Iterable[ddl.FieldSpec] = (),
    ):
        super().__init__(name)
        self._storage = storage
        self._metadata = NamedValueSet(metadata).freeze()
        self._uniqueKeys = NamedValueSet(uniqueKeys).freeze()

    def hasDependenciesIn(self, others: AbstractSet[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return False

    def visit(self, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        # Special handling for creating Dimension instances.
        dimension = GovernorDimension(
            self.name,
            storage=self._storage,
            metadata=self._metadata,
            uniqueKeys=self._uniqueKeys,
        )
        builder.dimensions.add(dimension)
        builder.elements.add(dimension)
