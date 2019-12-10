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

__all__ = ("Run", )

from .utils import slotValuesAreEqual, slotValuesToHash


class Run:
    """Represent a processing run.

    Parameters
    ----------
    collection : `str`
        A Collection name with which all Datasets in this Run are initially
        associated, also used as a human-readable name for this Run.
    startTime : `datetime`
        The start time for the run.
    endTime : `datetime`
        The end time for the run.
    host : `str`
        The system on which the run was produced.
    id : `int`, optional
        Unique integer identifier for this run.  Usually set to `None`
        (default) and assigned by `Registry`.
    """
    __slots__ = ("_collection", "_startTime", "_endTime", "_host", "_id")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    def __init__(self, collection, startTime=None, endTime=None, host=None, id=None):
        self._collection = collection
        self._id = id
        self._startTime = startTime
        self._endTime = endTime
        self._host = host

    def __repr__(self):
        return "Run(collection='{}', id={})".format(self.collection, self.id)

    @property
    def collection(self):
        return self._collection

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
