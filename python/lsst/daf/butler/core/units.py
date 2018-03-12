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

from collections import OrderedDict
from hashlib import sha512

__all__ = ("DataUnitMeta",
           "DataUnit",
           "DataUnitSet",
           "Camera",
           "AbstractFilter",
           "PhysicalFilter",
           "PhysicalSensor",
           "Visit",
           "ObservedSensor",
           "Snap",
           "VisitRange",
           "SkyMap",
           "Tract",
           "Patch")


class DataUnitMeta(type):
    """
    Metaclass for DataUnit, keeps track of all subclasses to enable
    construction by name.
    """
    def __init__(cls, name, bases, dct):  # noqa N805
        if not hasattr(cls, '_subclasses'):
            # this is the base class.
            cls._subclasses = {}
        else:
            cls._subclasses[name] = cls

        super(DataUnitMeta, cls).__init__(name, bases, dct)


class DataUnit(metaclass=DataUnitMeta):

    def insert(self, connection):
        raise NotImplementedError("pure virtual")

    def pkey(self):
        return NotImplemented("pure virtual")

    def invariantHash(self):
        """
        Compute a hash that is invariant across Python sessions, and hence
        can be stored in a database.

        Returns
        -------
        h : `sha512`
            Invariant hash of this instance.
        """
        return sha512(b''.join(str(v).encode('utf-8') for v in self.pkey)).digest()

    @staticmethod
    def find(values, connection):
        raise NotImplementedError("pure virtual")

    @classmethod
    def getType(cls, name):
        """
        Get type object for `DataUnit` subclass by *name*
        """
        if name not in cls._subclasses:
            raise ValueError("Unknown DataUnit: {}".format(name))
        return cls._subclasses[name]


def _buildGraph(unitTypes):
    """
    Recursively obtain all dependencies and add them to a single top-level
    dict representing all unique nodes in the graph.
    """
    nodes = {c.__name__: c for c in unitTypes}

    def addDependencies(node):
        for d in node.dependencies:
            if d.__name__ not in nodes:
                nodes[d.__name__] = d
                addDependencies(d)
    for c in list(nodes.values()):
        addDependencies(c)
    return nodes


def _sortTopological(unitTypes):
    """
    Standard DFS-based topological sort, but first recursively add
    all dependencies to the graph dictionary and visit them in
    lexographically sorted order to have deterministic output ordering.

    TODO: This surely can be done more efficiently (now effectively does
    DFS twice and does a sort), but it is tricky due to the required
    deterministic output ordering regardless of input.
    """
    graph = _buildGraph(unitTypes)
    sortedGraph = OrderedDict()
    for key in sorted(graph):
        sortedGraph[key] = graph[key]

    marked = {}
    results = []

    def visit(node):
        if node.__name__ in marked:
            return
        for d in node.dependencies:
            visit(d)
        marked[node.__name__] = True
        results.append(node)

    while sortedGraph:
        name, node = sortedGraph.popitem()
        visit(node)

    return results


class DataUnitSet(tuple):

    def __new__(cls, elements):
        return tuple.__new__(cls, _sortTopological(elements))

    def __hash__(self):
        return hash(tuple(item for item in self))

    def __eq__(self, other):
        if not isinstance(other, DataUnitSet):
            try:
                other = DataUnitSet(other)
            except Exception:
                return NotImplemented
        return super().__eq__(other)

    def __ne__(self, other):
        return not (self == other)

    def __contains__(self, key):
        for unit in self:
            if unit == key or unit.__class__.__name__ == key:
                return True
        return False

    def __getitem__(self, key):
        for unit in self:
            if unit.__class__.__name__ == key:
                return unit
        raise KeyError("DataUnit type with name {} not found".format(key))

    def invariantHash(self, values):
        """
        Compute a hash that is invariant across Python sessions, and hence
        can be stored in a database.
        """
        return sha512(b''.join(unit.invariantHash() for unit in values)).digest()

    def expand(self, findfunc, values):
        """Construct a dictionary of DataUnit instances from a dictionary
        of DataUnit primary key values.
        """
        result = {}
        for unitType in self:
            result[unitType.__name__] = findfunc(unitType, values)
        return result

    def conform(self, units):
        """Convert a sequence or dictionary of DataUnits to a tuple
        conforming to the ordering of this DataUnitSet.
        """
        if not isinstance(units, dict):
            units = {unit.__class__.__name__: unit for unit in units}
        return tuple((units[unitType.__name__] for unitType in self))


class Camera(DataUnit):

    dependencies = ()

    instances = {}

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name


class AbstractFilter(DataUnit):

    dependencies = ()


class PhysicalFilter(DataUnit):

    dependencies = (AbstractFilter, Camera)


class PhysicalSensor(DataUnit):

    dependencies = (Camera,)


class Visit(DataUnit):

    dependencies = (Camera, PhysicalFilter)


class ObservedSensor(DataUnit):

    dependencies = (Camera, PhysicalSensor, Visit)


class Snap(DataUnit):

    dependencies = (Camera, Visit)


class VisitRange(DataUnit):

    dependencies = (Camera,)


class SkyMap(DataUnit):

    dependencies = ()


class Tract(DataUnit):

    dependencies = (SkyMap,)


class Patch(DataUnit):

    dependencies = (SkyMap, Tract)
