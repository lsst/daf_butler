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

from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING

from .._named import NamedValueSet
from .._topology import TopologicalFamily, TopologicalSpace

if TYPE_CHECKING:
    from ._config import DimensionConfig
    from ._elements import Dimension, DimensionElement


class DimensionConstructionVisitor(ABC):
    """For adding entities to a builder class.

    An abstract base class for adding one or more entities to a
    `DimensionConstructionBuilder`.
    """

    @abstractmethod
    def has_dependencies_in(self, others: Set[str]) -> bool:
        """Test if dependencies have already been constructed.

        Tests whether other entities this visitor depends on have already
        been constructed.

        Parameters
        ----------
        others : `~collections.abc.Set` [ `str` ]
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
    def visit(self, name: str, builder: DimensionConstructionBuilder) -> None:
        """Modify the given builder object to include responsible entities.

        Parameters
        ----------
        name : `str`
            Name of the entity being added.
        builder : `DimensionConstructionBuilder`
            Builder to modify in-place and from which dependencies can be
            obtained.

        Notes
        -----
        Subclasses may assume (and callers must guarantee) that
        `hasDependenciesIn` would return `False` prior to `visit` being called.
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
    config : `DimensionConfig`
        The dimension universe to be used.
    namespace : `str`, optional
        The namespace to assign to this universe.
    """

    def __init__(
        self,
        version: int,
        commonSkyPixName: str,
        config: DimensionConfig,
        *,
        namespace: str | None = None,
    ) -> None:
        self.dimensions = NamedValueSet()
        self.elements = NamedValueSet()
        self.topology = {space: NamedValueSet() for space in TopologicalSpace.__members__.values()}
        self.version = version
        self.namespace = namespace
        self.config = config
        self.commonSkyPixName = commonSkyPixName
        self._todo: dict[str, DimensionConstructionVisitor] = {}

    def add(self, name: str, visitor: DimensionConstructionVisitor) -> None:
        """Add a single visitor to the builder.

        Parameters
        ----------
        name : `str`
            Name of the object the visitor creates.
        visitor : `DimensionConstructionVisitor`
            Visitor instance to add.
        """
        self._todo[name] = visitor

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
                if not visitor.has_dependencies_in(self._todo.keys())
            ]
            unblocked.sort()  # Break ties lexicographically.
            if not unblocked:
                raise RuntimeError(f"Cycle or unmet dependency in dimension elements: {self._todo.keys()}.")
            for name in unblocked:
                self._todo.pop(name).visit(name, self)

    version: int
    """Version number for the `DimensionUniverse` (`int`).

    Populated at builder construction.
    """

    namespace: str | None
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

    topology: dict[TopologicalSpace, NamedValueSet[TopologicalFamily]]
    """Dictionary containing all `TopologicalFamily` objects
    (`dict` [ `TopologicalSpace`, `NamedValueSet` [ `TopologicalFamily` ] ] ).
    """
