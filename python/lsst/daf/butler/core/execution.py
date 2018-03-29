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

from .utils import slotValuesAreEqual

__all__ = ("Execution",)


class Execution:
    """Any step in a production.

    Parameters
    ----------
    startTime : `datetime`
        The start time for the execution.  May have a different
        interpretation for different kinds of execution.
    endTime : `datetime`
        The end time for the execution.  May have a different
        interpretation for different kinds of execution.
    host : `str`
        The system on which the execution was run.  May have a different
        interpretation for different kinds of execution.
    id : `int`, optional
        Unique integer identifier for this Execution.
        Usually set to `None` (default) and assigned
        by `Registry.addExecution`.
    """

    __slots__ = ("_id", "_startTime", "_endTime", "_host")
    __eq__ = slotValuesAreEqual

    def __init__(self, startTime=None, endTime=None, host=None, id=None):
        self._id = id
        self._startTime = startTime
        self._endTime = endTime
        self._host = host

    @property
    def id(self):
        return self._id

    @property
    def startTime(self):
        return self._startTime

    @property
    def endTime(self):
        return self._endTime

    @property
    def host(self):
        return self._host
