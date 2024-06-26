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

__all__ = ["SkyPixRewriteVisitor"]

from typing import Any

from lsst.sphgeom import Region

from ..dimensions import DimensionUniverse, SkyPixDimension
from ..queries import tree as qt
from ..queries.tree._column_literal import IntColumnLiteral
from ..queries.visitors import ColumnExpressionVisitor, PredicateVisitFlags, SimplePredicateVisitor


class SkyPixRewriteVisitor(
    SimplePredicateVisitor,
    ColumnExpressionVisitor[tuple[SkyPixDimension, None] | tuple[None, Any] | tuple[None, None]],
):
    """A predicate visitor that rewrites skypix constraints that use non-common
    skypix.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Dimension universe.
    """

    def __init__(self, universe: DimensionUniverse):
        self.universe = universe
        self._common_skypix = universe.commonSkyPix
        self.region_constraints: list[Region] = []

    def visit_comparison(
        self,
        a: qt.ColumnExpression,
        operator: qt.ComparisonOperator,
        b: qt.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> qt.Predicate | None:
        if flags & PredicateVisitFlags.HAS_OR_SIBLINGS:
            return None
        if flags & PredicateVisitFlags.INVERTED:
            if operator == "!=":
                operator = "=="
            else:
                return None
        if operator == "==":
            k_a, v_a = a.visit(self)
            k_b, v_b = b.visit(self)
            if k_a is not None and v_b is not None:
                skypix_dimension = k_a
                value = v_b
            elif k_b is not None and v_a is not None:
                skypix_dimension = k_b
                value = v_a
            else:
                return None

            if skypix_dimension == self._common_skypix:
                # Common skypix should be handled properly, no need to rewrite.
                return None

            predicate: qt.Predicate | None = None
            region: Region | None = None
            if skypix_dimension.system.name == "htm" and self._common_skypix.system.name == "htm":
                # In case of HTM we can do some things in more optimal way.
                # TODO: This depends on HTM index mapping, maybe we should add
                # this facility to sphgeom classes.
                if skypix_dimension.level < self._common_skypix.level:
                    # In case of more coarse skypix we can just replace
                    # equality with a range constraint on a common skypix.
                    level_shift = (self._common_skypix.level - skypix_dimension.level) * 2
                    begin, end = (value << level_shift, ((value + 1) << level_shift) - 1)
                    predicate = qt.Predicate.in_range(
                        qt.DimensionKeyReference.model_construct(dimension=self._common_skypix), begin, end
                    )
                else:
                    # In case of a finer HTM we want to constraint on a common
                    # skypix and add post-processing filter for its region.
                    level_shift = (skypix_dimension.level - self._common_skypix.level) * 2
                    common_index = value >> level_shift
                    predicate = qt.Predicate.compare(
                        qt.DimensionKeyReference.model_construct(dimension=self._common_skypix),
                        "==",
                        IntColumnLiteral.model_construct(value=common_index),
                    )
                    region = skypix_dimension.pixelization.pixel(value)
            else:
                # More general case will use an envelope around the pixel
                # region, not super efficient.
                region = skypix_dimension.pixelization.pixel(value)
                # Try to limit the number of ranges, as it probably does not
                # help to have super-precise envelope.
                envelope = self._common_skypix.pixelization.envelope(region, 64)
                predicates: list[qt.Predicate] = []
                for begin, end in envelope:
                    if begin == end:
                        predicates.append(
                            qt.Predicate.compare(
                                qt.DimensionKeyReference.model_construct(dimension=self._common_skypix),
                                "==",
                                IntColumnLiteral.model_construct(value=begin),
                            )
                        )
                    else:
                        predicates.append(
                            qt.Predicate.in_range(
                                qt.DimensionKeyReference.model_construct(dimension=self._common_skypix),
                                begin,
                                end,
                            )
                        )
                predicate = qt.Predicate.from_bool(False).logical_or(*predicates)

            if region is not None:
                self.region_constraints.append(region)
            return predicate

        return None

    def visit_binary_expression(self, expression: qt.BinaryExpression) -> tuple[None, None]:
        return None, None

    def visit_unary_expression(self, expression: qt.UnaryExpression) -> tuple[None, None]:
        return None, None

    def visit_literal(self, expression: qt.ColumnLiteral) -> tuple[None, Any]:
        return None, expression.get_literal_value()

    def visit_dimension_key_reference(
        self, expression: qt.DimensionKeyReference
    ) -> tuple[SkyPixDimension, None] | tuple[None, None]:
        if isinstance(expression.dimension, SkyPixDimension):
            return expression.dimension, None
        else:
            return None, None

    def visit_dimension_field_reference(self, expression: qt.DimensionFieldReference) -> tuple[None, None]:
        return None, None

    def visit_dataset_field_reference(self, expression: qt.DatasetFieldReference) -> tuple[None, None]:
        return None, None

    def visit_reversed(self, expression: qt.Reversed) -> tuple[None, None]:
        raise AssertionError("No Reversed expressions in predicates.")
