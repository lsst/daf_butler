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
    "DatabaseDimension",
    "DatabaseDimensionCombination",
    "DatabaseDimensionElement",
    "DatabaseTopologicalFamily",
)

from collections.abc import Iterable, Mapping, Set
from types import MappingProxyType
from typing import TYPE_CHECKING

from lsst.utils.classes import cached_getter

from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalRelationshipEndpoint, TopologicalSpace
from ._elements import Dimension, DimensionCombination, DimensionElement, KeyColumnSpec, MetadataColumnSpec
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

if TYPE_CHECKING:
    from ..queries.tree import DimensionFieldReference
    from ._governor import GovernorDimension
    from ._group import DimensionGroup


class DatabaseTopologicalFamily(TopologicalFamily):
    """Database topological family implementation.

    A `TopologicalFamily` implementation for the `DatabaseDimension` and
    `DatabaseDimensionCombination` objects that have direct database
    representations.

    Parameters
    ----------
    name : `str`
        Name of the family.
    space : `TopologicalSpace`
        Space in which this family's regions live.
    members : `NamedValueAbstractSet` [ `DimensionElement` ]
        The members of this family, ordered according to the priority used
        in `choose` (first-choice member first).
    """

    def __init__(
        self,
        name: str,
        space: TopologicalSpace,
        *,
        members: NamedValueAbstractSet[DimensionElement],
    ):
        super().__init__(name, space)
        self.members = members

    def choose(self, dimensions: DimensionGroup) -> DimensionElement:
        # Docstring inherited from TopologicalFamily.
        for member in self.members:
            if member.name in dimensions.elements:
                return member
        raise RuntimeError(f"No recognized endpoints for {self.name} in {dimensions}.")

    @property
    @cached_getter
    def governor(self) -> GovernorDimension:
        """Return `GovernorDimension` common to all members of this family.

        (`GovernorDimension`).
        """
        governors = {m.governor for m in self.members}
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

    def make_column_reference(self, endpoint: TopologicalRelationshipEndpoint) -> DimensionFieldReference:
        # Docstring inherited from TopologicalFamily.
        from ..queries.tree import DimensionFieldReference

        assert isinstance(endpoint, DimensionElement)
        return DimensionFieldReference(
            element=endpoint,
            field=("region" if self.space is TopologicalSpace.SPATIAL else "timespan"),
        )

    members: NamedValueAbstractSet[DimensionElement]
    """The members of this family, ordered according to the priority used in
    `choose` (first-choice member first).
    """


class DatabaseTopologicalFamilyConstructionVisitor(DimensionConstructionVisitor):
    """A construction visitor for `DatabaseTopologicalFamily`.

    This visitor depends on (and is thus visited after) its members.

    Parameters
    ----------
    space : `TopologicalSpace`
        Space in which this family's regions live.
    members : `~collections.abc.Iterable` [ `str` ]
        The names of the members of this family, ordered according to the
        priority used in `choose` (first-choice member first).
    """

    def __init__(self, space: TopologicalSpace, members: Iterable[str]):
        self._space = space
        self._members = tuple(members)

    def has_dependencies_in(self, others: Set[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return not others.isdisjoint(self._members)

    def visit(self, name: str, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        members = NamedValueSet(builder.elements[member_name] for member_name in self._members)
        family = DatabaseTopologicalFamily(name, self._space, members=members.freeze())
        builder.topology[self._space].add(family)
        for member in members:
            assert isinstance(member, DatabaseDimension | DatabaseDimensionCombination)
            other = member._topology.setdefault(self._space, family)
            if other is not family:
                raise RuntimeError(
                    f"{member.name} is declared to be a member of (at least) two "
                    f"{self._space.name} families: {other.name} and {family.name}."
                )


class DatabaseDimensionElement(DimensionElement):
    """An intermediate base class for `DimensionElement` database classes.

    Instances of these element classes map directly to a database table or
    query.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    implied : `NamedValueAbstractSet` [ `Dimension` ]
        Other dimensions whose keys are included in this dimension's (logical)
        table as foreign keys.
    metadata_columns : `NamedValueAbstractSet` [ `MetadataColumnSpec` ]
        Field specifications for all non-key fields in this dimension's table.
    is_cached : `bool`
        Whether this element's records should be persistently cached in the
        client.
    doc : `str`
        Extended description of this element.
    """

    def __init__(
        self,
        name: str,
        *,
        implied: NamedValueAbstractSet[Dimension],
        metadata_columns: NamedValueAbstractSet[MetadataColumnSpec],
        is_cached: bool,
        doc: str,
    ):
        self._name = name
        self._implied = implied
        self._metadata_columns = metadata_columns
        self._topology: dict[TopologicalSpace, DatabaseTopologicalFamily] = {}
        self._is_cached = is_cached
        self._doc = doc

    @property
    def name(self) -> str:
        # Docstring inherited from TopologicalRelationshipEndpoint.
        return self._name

    @property
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._implied

    @property
    def metadata_columns(self) -> NamedValueAbstractSet[MetadataColumnSpec]:
        # Docstring inherited from DimensionElement.
        return self._metadata_columns

    @property
    def is_cached(self) -> bool:
        # Docstring inherited.
        return self._is_cached

    @property
    def documentation(self) -> str:
        # Docstring inherited from DimensionElement.
        return self._doc

    @property
    def topology(self) -> Mapping[TopologicalSpace, DatabaseTopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return MappingProxyType(self._topology)

    @property
    def spatial(self) -> DatabaseTopologicalFamily | None:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return self.topology.get(TopologicalSpace.SPATIAL)

    @property
    def temporal(self) -> DatabaseTopologicalFamily | None:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return self.topology.get(TopologicalSpace.TEMPORAL)


class DatabaseDimension(Dimension, DatabaseDimensionElement):
    """A `Dimension` class that maps directly to a database table or query.

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
    metadata_columns : `NamedValueAbstractSet` [ `MetadataColumnSpec` ]
        Field specifications for all non-key fields in this dimension's table.
    unique_keys : `NamedValueAbstractSet` [ `KeyColumnSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.
    implied_union_target : `str` or `None`
        If not `None`, the name of an element whose implied values for
        this element form the set of allowable values.
    is_cached : `bool`
        Whether this element's records should be persistently cached in the
        client.
    doc : `str`
        Extended description of this element.

    Notes
    -----
    `DatabaseDimension` objects may belong to a `TopologicalFamily`, but it is
    the responsibility of `DatabaseTopologicalFamilyConstructionVisitor` to
    update the `~TopologicalRelationshipEndpoint.topology` attribute of their
    members.
    """

    def __init__(
        self,
        name: str,
        *,
        required: NamedValueSet[Dimension],
        implied: NamedValueAbstractSet[Dimension],
        metadata_columns: NamedValueAbstractSet[MetadataColumnSpec],
        unique_keys: NamedValueAbstractSet[KeyColumnSpec],
        implied_union_target: str | None,
        is_cached: bool,
        doc: str,
    ):
        super().__init__(
            name, implied=implied, metadata_columns=metadata_columns, is_cached=is_cached, doc=doc
        )
        required.add(self)
        self._required = required.freeze()
        self._unique_keys = unique_keys
        self._implied_union_target = implied_union_target

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._required

    @property
    def unique_keys(self) -> NamedValueAbstractSet[KeyColumnSpec]:
        # Docstring inherited from Dimension.
        return self._unique_keys

    @property
    def implied_union_target(self) -> DimensionElement | None:
        # Docstring inherited from DimensionElement.
        # This is a bit encapsulation-breaking, but it'll all be cleaned up
        # soon when we get rid of the storage objects entirely.
        return self.universe[self._implied_union_target] if self._implied_union_target is not None else None


class DatabaseDimensionCombination(DimensionCombination, DatabaseDimensionElement):
    """A combination class that maps directly to a database table or query.

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
    metadata_columns : `NamedValueAbstractSet` [ `MetadataColumnSpec` ]
        Field specifications for all non-key fields in this combination's
        table.
    is_cached : `bool`
        Whether this element's records should be persistently cached in the
        client.
    always_join : `bool`, optional
        If `True`, always include this element in any query or data ID in
        which its ``required`` dimensions appear, because it defines a
        relationship between those dimensions that must always be satisfied.
    populated_by : `Dimension` or `None`
        The dimension that this element's records are always inserted,
        exported, and imported alongside.
    doc : `str`
        Extended description of this element.

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
        name: str,
        *,
        required: NamedValueAbstractSet[Dimension],
        implied: NamedValueAbstractSet[Dimension],
        metadata_columns: NamedValueAbstractSet[MetadataColumnSpec],
        is_cached: bool,
        always_join: bool,
        populated_by: Dimension | None,
        doc: str,
    ):
        super().__init__(
            name, implied=implied, metadata_columns=metadata_columns, is_cached=is_cached, doc=doc
        )
        self._required = required
        self._always_join = always_join
        self._populated_by = populated_by

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return self._required

    @property
    def alwaysJoin(self) -> bool:
        # Docstring inherited from DimensionElement.
        return self._always_join

    @property
    def defines_relationships(self) -> bool:
        # Docstring inherited from DimensionElement.
        return self._always_join or bool(self.implied)

    @property
    def populated_by(self) -> Dimension | None:
        # Docstring inherited.
        return self._populated_by
