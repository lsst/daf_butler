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

__all__ = ("GovernorDimension",)

from collections.abc import Mapping
from types import MappingProxyType

from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalSpace
from ._elements import Dimension, KeyColumnSpec, MetadataColumnSpec


class GovernorDimension(Dimension):
    """Governor dimension.

    A special `Dimension` with no dependencies and a small number of rows,
    used to group the dimensions that depend on it.

    Parameters
    ----------
    name : `str`
        Name of the dimension.
    metadata_columns : `NamedValueAbstractSet` [ `MetadataColumnSpec` ]
        Field specifications for all non-key fields in this dimension's table.
    unique_keys : `NamedValueAbstractSet` [ `KeyColumnSpec` ]
        Fields that can each be used to uniquely identify this dimension (given
        values for all required dimensions).  The first of these is used as
        (part of) this dimension's table's primary key, while others are used
        to define unique constraints.
    doc : `str`
        Extended description of this element.

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
        *,
        metadata_columns: NamedValueAbstractSet[MetadataColumnSpec],
        unique_keys: NamedValueAbstractSet[KeyColumnSpec],
        doc: str,
    ):
        self._name = name
        self._required = NamedValueSet({self}).freeze()
        self._metadata_columns = metadata_columns
        self._unique_keys = unique_keys
        self._doc = doc
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
    def metadata_columns(self) -> NamedValueAbstractSet[MetadataColumnSpec]:
        # Docstring inherited from DimensionElement.
        return self._metadata_columns

    @property
    def unique_keys(self) -> NamedValueAbstractSet[KeyColumnSpec]:
        # Docstring inherited from Dimension.
        return self._unique_keys

    @property
    def is_cached(self) -> bool:
        # Docstring inherited.
        return True

    @property
    def documentation(self) -> str:
        # Docstring inherited from DimensionElement.
        return self._doc
