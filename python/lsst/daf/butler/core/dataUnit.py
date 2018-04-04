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

__all__ = ("DataUnit", )


class DataUnit:
    """A discrete abstract unit of data that can be associated with
    metadata and used to label datasets.
    
    `DataUnit` instances represent concrete units such as e.g. `Camera`,
    `Sensor`, `Visit` and `SkyMap`.

    Attributes
    ----------
    requiredDependencies : `frozenset`
        Related `DataUnit` instances on which existence this `DataUnit`
        instance depends.
    optionalDependencies : `frozenset`
        Related `DataUnit` instances that may also be provided (and when they
        are, they must be kept in sync).
    dependencies : `frozenset`
        The union of `requiredDependencies` and `optionalDependencies`.
    table : `sqlalchemy.core.Table`, optional
        When not ``None`` the primary table entry corresponding to this
        `DataUnit`.
    """
    @property
    def requiredDependencies(self):
        return self._requiredDependencies

    @property
    def optionalDependencies(self):
        return self._optionalDependencies

    @property
    def dependencies(self):
        return self.requiredDependencies.union(self.optionalDependencies)

    @property
    def table(self):
        if hasattr(self, '_table'):
            return self._table
        else:
            return None
