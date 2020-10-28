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

__all__ = (
    "DatabaseDimension",
    "DatabaseDimensionCombination",
    "DatabaseDimensionElement",
)

from types import MappingProxyType
from typing import (
    AbstractSet,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Set,
    TYPE_CHECKING,
)

from .. import ddl
from ..named import NamedValueAbstractSet, NamedValueSet
from ..utils import cached_getter
from .._topology import TopologicalFamily, TopologicalRelationshipEndpoint, TopologicalSpace

from ._elements import Dimension, DimensionCombination, DimensionElement
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

if TYPE_CHECKING:
    from ._governor import GovernorDimension


class DatabaseTopologicalFamily(TopologicalFamily):
    """A `TopologicalFamily` implementation for the `DatabaseDimension` and
    `DatabaseDimensionCombination` objects that have direct database
    representations.

    Parameters
    ----------
    name : `str`
        Name of the family.
    space : `TopologicalSpace`
        Space in which this families regions live.
    members : `NamedValueAbstractSet` [ `DimensionElement` ]
        The members of this family, ordered according to the priority used
        in `choose` (first-choice member first).
    """
    def __init__(
        self,
        name: str,
        space: TopologicalSpace, *,
        members: NamedValueAbstractSet[DimensionElement],
    ):
        super().__init__(name, space)
        self.members = members

    def choose(self, endpoints: NamedValueAbstractSet[TopologicalRelationshipEndpoint]) -> DimensionElement:
        # Docstring inherited from TopologicalFamily.
        for member in self.members:
            if member in endpoints:
                return member
        raise RuntimeError(f"No recognized endpoints for {self.name} in {endpoints}.")

    @property  # type: ignore
    @cached_getter
    def governor(self) -> GovernorDimension:
        """The `GovernorDimension` common to all members of this family
        (`GovernorDimension`).
        """
        governors = set(m.governor for m in self.members)
        if None in governors:
            raise RuntimeError(
                f"Bad {self.space.name} family definition {self.name}: at least one member "
                f"in {self.members} has no GovernorDimension dependency."
            )
        try:
            (result,) = governors
        except ValueError:
            raise RuntimeError(
                f"Bad {self.space.name} family definition {self.name}: multiple governors {governors} "
                f"in {self.members}."
            ) from None
        return result  # type: ignore

    members: NamedValueAbstractSet[DimensionElement]
    """The members of this family, ordered according to the priority used in
    `choose` (first-choice member first).
    """


class DatabaseTopologicalFamilyConstructionVisitor(DimensionConstructionVisitor):
    """A construction visitor for `DatabaseTopologicalFamily`.

    This visitor depends on (and is thus visited after) its members.

    Parameters
    ----------
    name : `str`
        Name of the family.
    members : `Iterable` [ `str` ]
        The names of the members of this family, ordered according to the
        priority used in `choose` (first-choice member first).
    """
    def __init__(self, name: str, space: TopologicalSpace, members: Iterable[str]):
        super().__init__(name)
        self._space = space
        self._members = tuple(members)

    def hasDependenciesIn(self, others: AbstractSet[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return not others.isdisjoint(self._members)

    def visit(self, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        members = NamedValueSet(builder.elements[name] for name in self._members)
        family = DatabaseTopologicalFamily(
            self.name,
            self._space,
            members=members.freeze()
        )
        builder.topology[self._space].add(family)
        for member in members:
            assert isinstance(member, (DatabaseDimension, DatabaseDimensionCombination))
            other = member._topology.setdefault(self._space, family)
            if other is not family:
                raise RuntimeError(f"{member.name} is declared to be a member of (at least) two "
                                   f"{self._space.name} families: {other.name} and {family.name}.")


class DatabaseDimensionElement(DimensionElement):
    """An intermediate base class for `DimensionElement` classes whose
    instances that map directly to a database table or query.
    """

    def __init__(self, name: str):
        super().__init__(name)
        self._topology: Dict[TopologicalSpace, DatabaseTopologicalFamily] = {}

    @property
    def topology(self) -> Mapping[TopologicalSpace, DatabaseTopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return MappingProxyType(self._topology)

    @property
    def spatial(self) -> Optional[DatabaseTopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return self.topology.get(TopologicalSpace.SPATIAL)

    @property
    def temporal(self) -> Optional[DatabaseTopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return self.topology.get(TopologicalSpace.TEMPORAL)


class DatabaseDimension(Dimension, DatabaseDimensionElement):
    """A `Dimension` implementation that maps directly to a database table or
    query.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    required : `NamedValueSet` [ `Dimension` ]
        Other dimensions whose keys are part of the compound primary key for
        this dimension's (logical) table, as well as references to their own
        tables.  The ``required`` parameter does not include ``self`` (it
        can't, of course), but the corresponding attribute does - it is added
        by the constructor.
    implied : `NamedValueAbstractSet` [ `Dimension` ]
        Other dimensions whose keys are included in this dimension's (logical)
        table as foreign keys.
    metadata : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
        Field specifications for all non-key fields in this dimension's table.
    cached : `bool`
        Whether to cache the records of this dimension in memory when they are
        fetched from the database.
    viewOf : `str`, optional
        Name of another `DatabaseDimension` or `DatabaseDimensionCombination`
        whose records this dimension's records summarize.
    uniqueKeys : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.

    Notes
    -----
    `DatabaseDimension` objects may belong to a `TopologicalFamily`, but it is
    the responsibility of `DatabaseTopologicalFamilyConstructionVisitor` to
    update the `~TopologicalRelationshipEndpoint.topology` attribute of their
    members.
    """
    def __init__(
        self,
        name: str, *,
        required: NamedValueSet[Dimension],
        implied: NamedValueAbstractSet[Dimension],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
        cached: bool,
        viewOf: Optional[str],
        uniqueKeys: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        super().__init__(name)
        required.add(self)
        self._required = required.freeze()
        self._implied = implied
        self._metadata = metadata
        self._cached = cached
        self._viewOf = viewOf
        self._uniqueKeys = uniqueKeys

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._required

    @property
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._implied

    @property
    def metadata(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from DimensionElement.
        return self._metadata

    @property
    def cached(self) -> bool:
        # Docstring inherited from DimensionElement.
        return self._cached

    @property
    def viewOf(self) -> Optional[str]:
        # Docstring inherited from DimensionElement.
        return self._viewOf

    @property
    def uniqueKeys(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from Dimension.
        return self._uniqueKeys


class DatabaseDimensionCombination(DimensionCombination, DatabaseDimensionElement):
    """A `DimensionCombination` implementation that maps directly to a database
    table or query.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    required : `NamedValueAbstractSet` [ `Dimension` ]
        Dimensions whose keys define the compound primary key for this
        combinations's (logical) table, as well as references to their own
        tables.
    implied : `NamedValueAbstractSet` [ `Dimension` ]
        Dimensions whose keys are included in this combinations's (logical)
        table as foreign keys.
    metadata : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
        Field specifications for all non-key fields in this combination's
        table.
    cached : `bool`
        Whether to cache the records of this combination in memory when they
        are fetched from the database.
    viewOf : `str`, optional
        Name of another `DatabaseDimension` or `DatabaseDimensionCombination`
        whose records this combination's records summarize.
    alwaysJoin : `bool`, optional
        If `True`, always include this element in any query or data ID in
        which its ``required`` dimensions appear, because it defines a
        relationship between those dimensions that must always be satisfied.

    Notes
    -----
    `DatabaseDimensionCombination` objects may belong to a `TopologicalFamily`,
    but it is the responsibility of
    `DatabaseTopologicalFamilyConstructionVisitor` to update the
    `~TopologicalRelationshipEndpoint.topology` attribute of their members.

    This class has a lot in common with `DatabaseDimension`, but they are
    expected to diverge in future changes, and the only way to make them share
    method implementations would be via multiple inheritance.  Given the
    trivial nature of all of those implementations, this does not seem worth
    the drawbacks (particularly the constraints it imposes on constructor
    signatures).
    """
    def __init__(
        self,
        name: str, *,
        required: NamedValueAbstractSet[Dimension],
        implied: NamedValueAbstractSet[Dimension],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
        cached: bool,
        viewOf: Optional[str],
        alwaysJoin: bool,
    ):
        super().__init__(name)
        self._required = required
        self._implied = implied
        self._topology: Dict[TopologicalSpace, DatabaseTopologicalFamily] = {}
        self._metadata = metadata
        self._cached = cached
        self._viewOf = viewOf
        self._alwaysJoin = alwaysJoin

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._required

    @property
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._implied

    @property
    def topology(self) -> Mapping[TopologicalSpace, DatabaseTopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return MappingProxyType(self._topology)

    @property
    def metadata(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from DimensionElement.
        return self._metadata

    @property
    def cached(self) -> bool:
        # Docstring inherited from DimensionElement.
        return self._cached

    @property
    def viewOf(self) -> Optional[str]:
        # Docstring inherited from DimensionElement.
        return self._viewOf

    @property
    def alwaysJoin(self) -> bool:
        # Docstring inherited from DimensionElement.
        return self._alwaysJoin


class DatabaseDimensionElementConstructionVisitor(DimensionConstructionVisitor):
    """A construction visitor for `DatabaseDimension` and
    `DatabaseDimensionCombination`.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    required : `Set` [ `Dimension` ]
        Names of dimensions whose keys define the compound primary key for this
        element's (logical) table, as well as references to their own
        tables.
    implied : `Set` [ `Dimension` ]
        Names of dimension whose keys are included in this elements's
        (logical) table as foreign keys.
    metadata : `Iterable` [ `ddl.FieldSpec` ]
        Field specifications for all non-key fields in this element's table.
    cached : `bool`
        Whether to cache the records of this element in memory when they
        are fetched from the database.
    viewOf : `str`, optional
        Name of another `DatabaseDimension` or `DatabaseDimensionCombination`
        whose records this element's records summarize.
    uniqueKeys : `Iterable` [ `ddl.FieldSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.  Should be empty for
        `DatabaseDimensionCombination` definitions.
    alwaysJoin : `bool`, optional
        If `True`, always include this element in any query or data ID in
        which its ``required`` dimensions appear, because it defines a
        relationship between those dimensions that must always be satisfied.
        Should only be provided when a `DimensionCombination` is being
        constructed.
    """
    def __init__(
        self,
        name: str,
        required: Set[str],
        implied: Set[str],
        metadata: Iterable[ddl.FieldSpec] = (),
        cached: bool = False,
        viewOf: Optional[str] = None,
        uniqueKeys: Iterable[ddl.FieldSpec] = (),
        alwaysJoin: bool = False,
    ):
        super().__init__(name)
        self._required = required
        self._implied = implied
        self._metadata = NamedValueSet(metadata).freeze()
        self._cached = cached
        self._viewOf = viewOf
        self._uniqueKeys = NamedValueSet(uniqueKeys).freeze()
        self._alwaysJoin = alwaysJoin

    def hasDependenciesIn(self, others: AbstractSet[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return not (
            self._required.isdisjoint(others)
            and self._implied.isdisjoint(others)
        )

    def visit(self, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        # Expand required dependencies.
        for name in tuple(self._required):  # iterate over copy
            self._required.update(builder.dimensions[name].required.names)
        # Transform required and implied Dimension names into instances,
        # and reorder to match builder's order.
        required: NamedValueSet[Dimension] = NamedValueSet()
        implied: NamedValueSet[Dimension] = NamedValueSet()
        for dimension in builder.dimensions:
            if dimension.name in self._required:
                required.add(dimension)
            if dimension.name in self._implied:
                implied.add(dimension)

        if self._uniqueKeys:
            if self._alwaysJoin:
                raise RuntimeError(f"'alwaysJoin' is not a valid option for Dimension object {self.name}.")
            # Special handling for creating Dimension instances.
            dimension = DatabaseDimension(
                self.name,
                required=required,
                implied=implied.freeze(),
                metadata=self._metadata,
                cached=self._cached,
                viewOf=self._viewOf,
                uniqueKeys=self._uniqueKeys,
            )
            builder.dimensions.add(dimension)
            builder.elements.add(dimension)
        else:
            # Special handling for creating DimensionCombination instances.
            combination = DatabaseDimensionCombination(
                self.name,
                required=required,
                implied=implied.freeze(),
                metadata=self._metadata,
                cached=self._cached,
                viewOf=self._viewOf,
                alwaysJoin=self._alwaysJoin,
            )
            builder.elements.add(combination)
