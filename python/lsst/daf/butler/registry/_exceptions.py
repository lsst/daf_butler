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


__all__ = (
    "ArgumentError",
    "CollectionExpressionError",
    "ConflictingDefinitionError",
    "DatasetTypeExpressionError",
    "MissingSpatialOverlapError",
    "NoDefaultCollectionError",
    "OrphanedRecordError",
    "RegistryConsistencyError",
    "UnsupportedIdGeneratorError",
    "UserExpressionError",
    "UserExpressionSyntaxError",
)

from .._exceptions_legacy import CollectionError, RegistryError


class ArgumentError(RegistryError):
    """Exception raised when method arguments are invalid or inconsistent."""


class DatasetTypeExpressionError(RegistryError):
    """Exception raised for an incorrect dataset type expression."""


class CollectionExpressionError(CollectionError):
    """Exception raised for an incorrect collection expression."""


class NoDefaultCollectionError(CollectionError):
    """Exception raised when a collection is needed, but collection argument
    is not provided and default collection is not defined in registry.
    """


class UserExpressionError(RegistryError):
    """Exception raised for problems with user expression."""


class UserExpressionSyntaxError(UserExpressionError):
    """Exception raised when a user query expression cannot be parsed."""


class ConflictingDefinitionError(RegistryError):
    """Exception raised when trying to insert a database record when a
    conflicting record already exists.
    """


class OrphanedRecordError(RegistryError):
    """Exception raised when trying to remove or modify a database record
    that is still being used in some other table.
    """


class UnsupportedIdGeneratorError(ValueError):
    """Exception raised when an unsupported `DatasetIdGenEnum` option is
    used for insert/import.
    """


class MissingSpatialOverlapError(RegistryError):
    """Exception raised when a spatial overlap relationship needed by a query
    has not been precomputed and cannot be computed on-the-fly.
    """


class RegistryConsistencyError(RegistryError):
    """Exception raised when an internal registry consistency check fails,
    usually means bug in the Regitry itself.
    """
