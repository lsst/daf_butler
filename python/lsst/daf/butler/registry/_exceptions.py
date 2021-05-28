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


__all__ = (
    "ConflictingDefinitionError",
    "InconsistentDataIdError",
    "MissingCollectionError",
    "OrphanedRecordError",
    "UnsupportedIdGeneratorError",
)

# This exception moved for intra-daf_butler dependency reasons; import here and
# re-export for backwards compatibility.
from ..core import InconsistentDataIdError


class ConflictingDefinitionError(Exception):
    """Exception raised when trying to insert a database record when a
    conflicting record already exists.
    """


class OrphanedRecordError(Exception):
    """Exception raised when trying to remove or modify a database record
    that is still being used in some other table.
    """


class MissingCollectionError(Exception):
    """Exception raised when an operation attempts to use a collection that
    does not exist.
    """


class UnsupportedIdGeneratorError(ValueError):
    """Exception raised when an unsupported `DatasetIdGenEnum` option is
    used for insert/import.
    """
