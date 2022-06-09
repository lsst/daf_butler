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

__all__ = ("LocalConstraints",)

import dataclasses
from collections import defaultdict
from typing import ClassVar, Iterable, Mapping, Optional

from lsst.sphgeom import Region
from lsst.utils.iteration import ensure_iterable
from lsst.utils.sets.ellipsis import EllipsisType
from lsst.utils.sets.unboundable import FrozenUnboundableSet, MutableUnboundableSet, UnboundableSet

from .._spatial_regions import SpatialConstraint
from ..dimensions import DataCoordinate, DataIdValue
from ..timespan import TemporalConstraint, Timespan


def _make_dimensions_dict(
    **kwargs: UnboundableSet[DataIdValue],
) -> defaultdict[str, UnboundableSet[DataIdValue]]:
    return defaultdict(FrozenUnboundableSet.make_full, {k: v.frozen() for k, v in kwargs.items()})


@dataclasses.dataclass(frozen=True, slots=True, eq=False)
class LocalConstraints:
    dimensions: Mapping[str, UnboundableSet[DataIdValue]]
    spatial: SpatialConstraint
    temporal: TemporalConstraint

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", _make_dimensions_dict(**self.dimensions))

    _full: ClassVar[Optional[LocalConstraints]] = None

    @classmethod
    def make_full(cls) -> LocalConstraints:
        if cls._full is None:
            cls._full = cls(
                {},
                SpatialConstraint.make_full(),
                TemporalConstraint.make_full(),
            )
        return cls._full

    @classmethod
    def from_misc(
        cls,
        *,
        data_id: Optional[DataCoordinate] = None,
        region: Optional[Region] = None,
        timespan: Optional[Timespan] = None,
        spatial: Optional[SpatialConstraint] = None,
        temporal: Optional[TemporalConstraint] = None,
        dimensions: Optional[
            Mapping[str, DataIdValue | Iterable[DataIdValue] | EllipsisType | UnboundableSet[DataIdValue]]
        ] = None,
    ) -> LocalConstraints:
        # Assemble dimension bounds from data_id arg and kwargs.
        conformed_dimensions: dict[str, UnboundableSet[DataIdValue]] = {}
        if data_id is not None:
            if data_id.hasFull():
                maximal_data_id = data_id.full.byName()
            else:
                maximal_data_id = data_id.byName()
            for key, value in maximal_data_id.items():
                conformed_dimensions[key] = FrozenUnboundableSet(frozenset({value}))
        if dimensions is not None:
            for key, value_or_values in dimensions.items():
                match value_or_values:
                    case EllipsisType():
                        conformed_dimensions[key] = FrozenUnboundableSet.full
                    case UnboundableSet():
                        conformed_dimensions[key] = value_or_values.frozen()
                    case _:
                        conformed_dimensions[key] = FrozenUnboundableSet(
                            frozenset(ensure_iterable(value_or_values))
                        )
        # Assemble spatial constraint from spatial and region args.
        if region is not None:
            if spatial is not None:
                raise TypeError("At most one of 'spatial' and 'region' may be provided.")
            spatial = SpatialConstraint(region)
        if spatial is None:
            spatial = SpatialConstraint.make_full()
        # Assemble temporal constraint from temporal and timespan args.
        if timespan is not None:
            if temporal is not None:
                raise TypeError("At most one of 'temporal' and 'timespan' may be provided.")
            temporal = TemporalConstraint.from_timespan(timespan)
        if temporal is None:
            temporal = TemporalConstraint.make_full()
        return cls(conformed_dimensions, spatial, temporal)

    def union(*args: LocalConstraints) -> LocalConstraints:
        # Optimize common case where there are no constraints and make_full
        # was used to construct instances.
        full = LocalConstraints.make_full()
        if any(arg is full for arg in args):
            return full
        dimensions: defaultdict[str, MutableUnboundableSet] = defaultdict(MutableUnboundableSet.make_empty)
        all_spatial: list[SpatialConstraint] = []
        all_temporal: list[TemporalConstraint] = []
        for arg in args:
            for dimension, bounds in arg.dimensions.items():
                dimensions[dimension].update(bounds)
            all_spatial.append(arg.spatial)
            all_temporal.append(arg.temporal)
        return LocalConstraints(
            _make_dimensions_dict(**dimensions),  # need to change the default to unbounded
            SpatialConstraint.union(*all_spatial),
            TemporalConstraint.union(*all_temporal),
        )

    def intersection(*args: LocalConstraints) -> LocalConstraints:
        full = LocalConstraints.make_full()
        dimensions: defaultdict[str, MutableUnboundableSet] = defaultdict(MutableUnboundableSet.make_full)
        all_spatial: list[SpatialConstraint] = []
        all_temporal: list[TemporalConstraint] = []
        for arg in args:
            # Optimize common case where there are no constraints and make_full
            # was used to construct instances.
            if arg is full:
                continue
            for dimension, bounds in arg.dimensions.items():
                dimensions[dimension].intersection_update(bounds)
            all_spatial.append(arg.spatial)
            all_temporal.append(arg.temporal)
        return LocalConstraints(
            dimensions,
            SpatialConstraint.intersection(*all_spatial),
            TemporalConstraint.intersection(*all_temporal),
        )
