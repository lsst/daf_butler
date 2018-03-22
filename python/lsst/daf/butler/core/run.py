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

from .utils import slotValuesAreEqual, slotValuesToHash

__all__ = ("Run", )


class Run(object):
    """Represent a processing run.

    Parameters
    ----------
    execution : `Execution`
        The Execution for this Run.
    collection : `str`
        A Collection name with which all Datasets in this Run are initially
        associated, also used as a human-readable name for this Run.
    environment : `DatasetRef`
        A reference to a dataset that contains a description of
        the software environment (e.g. versions) used for this Run.
    pipeline : `DatasetRef`
        A reference to a dataset that contains a serialization of
        the SuperTask Pipeline used for this Run (if any).
    """
    __slots__ = ("_execution", "_collection", "_environment", "_pipeline")
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    def __init__(self, execution, collection, environment, pipeline):
        self._execution = execution
        self._collection = collection
        self._environment = environment
        self._pipeline = pipeline

    @property
    def execution(self):
        return self._execution

    @property
    def collection(self):
        return self._collection

    @property
    def environment(self):
        return self._environment

    @property
    def pipeline(self):
        return self._pipeline
