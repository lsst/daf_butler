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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Dict, Iterable, Optional

from .._topology import TopologicalFamily, TopologicalSpace
from ..named import NamedValueSet

if TYPE_CHECKING:
    from ._config import DimensionConfig
    from ._elements import Dimension, DimensionElement
    from ._packer import DimensionPackerFactory


class DimensionConstructionVisitor(ABC):
    """For adding entities to a builder class.

    An abstract base class for adding one or more entities to a
    `DimensionConstructionBuilder`.

    Parameters
    ----------
    name : `str`
        Name of an entity being added.  This must be unique across all
        entities, which include `DimensionElement`, `TopologicalFamily`, and
        `DimensionPackerFactory` objects.  The visitor may add other entities
        as well, as long as only the named entity is referenced by other
        entities in the universe.
    """

    def __init__(self, name: str):
        self.name = name

    def __str__(self) -> str:
        return self.name

    @abstractmethod
    def hasDependenciesIn(self, others: AbstractSet[str]) -> bool:
        """Test if dependencies have already been constructed.

        Tests whether other entities this visitor depends on have already
        been constructed.

        Parameters
        ----------
        others : `AbstractSet` [ `str` ]
            The names of other visitors that have not yet been invoked.

        Returns
        -------
        blocked : `bool`
            If `True`, this visitor has dependencies on other visitors that
            must be invoked before this one can be.  If `False`, `visit` may
            be called.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit(self, builder: DimensionConstructionBuilder) -> None:
        """Modify the given builder object to include responsible entities.

        Parameters
        ----------
        builder : `DimensionConstructionBuilder`
            Builder to modify in-place and from which dependencies can be
            obtained.

        Notes
        -----
        Subclasses may assume (and callers must guarantee) that
        `hasDependenciesIn` would return `False` prior to `visit` being
        called.
        """
        raise NotImplementedError()


class DimensionConstructionBuilder:
    """A builder object for constructing `DimensionUniverse` instances.

    `DimensionConstructionVisitor` objects can be added to a
    `DimensionConstructionBuilder` object in any order, and are invoked
    in a deterministic order consistent with their dependency relationships
    by a single call (by the `DimensionUniverse`) to the `finish` method.

    Parameters
    ----------
    version : `int`
        Version for the `DimensionUniverse`.
    commonSkyPixName : `str`
        Name of the "common" skypix dimension that is used to relate all other
        spatial `TopologicalRelationshipEndpoint` objects.
    namespace : `str`, optional
        The namespace to assign to this universe.
    visitors : `Iterable` [ `DimensionConstructionVisitor` ]
        Visitor instances to include from the start.
    """

    def __init__(
        self,
        version: int,
        commonSkyPixName: str,
        config: DimensionConfig,
        *,
        namespace: Optional[str] = None,
        visitors: Iterable[DimensionConstructionVisitor] = (),
    ) -> None:
        self.dimensions = NamedValueSet()
        self.elements = NamedValueSet()
        self.topology = {space: NamedValueSet() for space in TopologicalSpace.__members__.values()}
        self.packers = {}
        self.version = version
        self.namespace = namespace
        self.config = config
        self.commonSkyPixName = commonSkyPixName
        self._todo: Dict[str, DimensionConstructionVisitor] = {v.name: v for v in visitors}

    def add(self, visitor: DimensionConstructionVisitor) -> None:
        """Add a single visitor to the builder.

        Parameters
        ----------
        visitor : `DimensionConstructionVisitor`
            Visitor instance to add.
        """
        self._todo[visitor.name] = visitor

    def update(self, visitors: Iterable[DimensionConstructionVisitor]) -> None:
        """Add multiple visitors to the builder.

        Parameters
        ----------
        visitors : `Iterable` [ `DimensionConstructionVisitor` ]
            Visitor instances to add.
        """
        self._todo.update((v.name, v) for v in visitors)

    def finish(self) -> None:
        """Complete construction of the builder.

        This method invokes all visitors in an order consistent with their
        dependencies, fully populating all public attributes.  It should be
        called only once, by `DimensionUniverse` itself.
        """
        while self._todo:
            unblocked = [
                name
                for name, visitor in self._todo.items()
                if not visitor.hasDependenciesIn(self._todo.keys())
            ]
            unblocked.sort()  # Break ties lexicographically.
            if not unblocked:
                raise RuntimeError(f"Cycle or unmet dependency in dimension elements: {self._todo.keys()}.")
            for name in unblocked:
                self._todo.pop(name).visit(self)

    version: int
    """Version number for the `DimensionUniverse` (`int`).

    Populated at builder construction.
    """

    namespace: Optional[str]
    """Namespace for the `DimensionUniverse` (`str`)

    Populated at builder construction.
    """

    commonSkyPixName: str
    """Name of the common skypix dimension used to connect other spatial
    `TopologicalRelationshipEndpoint` objects (`str`).

    Populated at builder construction.
    """

    dimensions: NamedValueSet[Dimension]
    """Set of all `Dimension` objects (`NamedValueSet` [ `Dimension` ]).

    Populated by `finish`.
    """

    elements: NamedValueSet[DimensionElement]
    """Set of all `DimensionElement` objects
    (`NamedValueSet` [ `DimensionElement` ]).

    Populated by `finish`.  `DimensionConstructionVisitor` classes that
    construct `Dimension` objects are responsible for adding them to this
    set as well as `dimensions`.
    """

    topology: Dict[TopologicalSpace, NamedValueSet[TopologicalFamily]]
    """Dictionary containing all `TopologicalFamily` objects
    (`dict` [ `TopologicalSpace`, `NamedValueSet` [ `TopologicalFamily` ] ] ).
    """

    packers: Dict[str, DimensionPackerFactory]
    """Dictionary containing all `DimensionPackerFactory` objects
    (`dict` [ `str`, `DimensionPackerFactory` ] ).
    """
