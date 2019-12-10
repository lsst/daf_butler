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

from .execution import Execution
from .utils import slotValuesAreEqual, slotValuesToHash


class Run(Execution):
    """Represent a processing run.

    Parameters
    ----------
    collection : `str`
        A Collection name with which all Datasets in this Run are initially
        associated, also used as a human-readable name for this Run.
    """
    __slots__ = ("_collection",)
    __eq__ = slotValuesAreEqual
    __hash__ = slotValuesToHash

    def __init__(self, collection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._collection = collection

    def __repr__(self):
        return "Run(collection='{}', id={})".format(self.collection, self.id)

    @property
    def collection(self):
        return self._collection
