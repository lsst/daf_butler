#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

from collections import OrderedDict
from hashlib import sha512
from datetime import datetime  # placeholder while prototyping
from sqlalchemy.sql import select, and_, exists
from .schema import CameraTable, AbstractFilterTable, PhysicalFilterTable, PhysicalSensorTable, VisitTable, ObservedSensorTable, SnapTable, VisitRangeTable, SkyMapTable, TractTable, PatchTable


class DataUnitMeta(type):
    """
    Metaclass for DataUnit, keeps track of all subclasses to enable construction by name.
    """
    def __init__(cls, name, bases, dct):
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
        Compute a hash that is invariant across Python sessions, and hence can be stored in a database.
        """
        return sha512(b''.join(str(v).encode('utf-8') for v in self.pkey)).digest()

    @staticmethod
    def find(values, connection):
        raise NotImplementedError("pure virtual")

    @classmethod
    def getType(cls, name):
        """
        Get type object for DataUnit subclass by *name*
        """
        assert isinstance(name, str)
        if name not in cls._subclasses:
            raise ValueError("Unknown DataUnit: {}".format(name))
        return cls._subclasses[name]


def _buildGraph(unitTypes):
    """
    Recursively obtain all dependencies and add them to a single top-level dict
    representing all unique nodes in the graph.
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
    all dependencies to the graph dictionary and visit them in lexographically
    sorted order to have deterministic output ordering.

    TODO: This surely can be done more efficiently (now effectively does DFS twice
    and does a sort), but it is tricky due to the required deterministic output ordering
    regardless of input.
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


class DataUnitTypeSet(tuple):

    def __new__(cls, elements):
        return tuple.__new__(cls, _sortTopological(elements))

    def __hash__(self):
        return hash(tuple(item for item in self))

    def __eq__(self, other):
        if not isinstance(other, DataUnitTypeSet):
            try:
                other = DataUnitTypeSet(other)
            except:
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
        Compute a hash that is invariant across Python sessions, and hence can be stored in a database.
        """
        return sha512(b''.join(unit.invariantHash() for unit in values)).digest()

    def expand(self, findfunc, values):
        """Construct a dictionary of DataUnit instances from a dictionary of DataUnit primary key values.
        """
        result = {}
        for unitType in self:
            result[unitType.__name__] = findfunc(unitType, values)
        return result

    def conform(self, units):
        """Convert a sequence or dictionary of DataUnits to a tuple conforming to the ordering of this DataUnitTypeSet.
        """
        if not isinstance(units, dict):
            units = {unit.__class__.__name__: unit for unit in units}
        return tuple((units[unitType.__name__] for unitType in self))


class Camera(DataUnit):

    dependencies = ()

    __slots__ = ("_name", "_module")

    instances = {}

    def __init__(self, name, module=""):
        self._name = name
        self._module = module

    def __hash__(self):
        return hash((self._name, self._module))

    def __eq__(self, other):
        return self._name == other._name and self._module == other._module

    @property
    def name(self):
        return self._name

    @property
    def module(self):
        return self._module

    @property
    def value(self):
        return self.name

    @property
    def pkey(self):
        return self.value

    def makePhysicalSensors(self):
        raise NotImplementedError("pure virtual")

    def makePhysicalFilters(self):
        raise NotImplementedError("pure virtual")

    def __repr__(self):
        return "{'Camera': '%s'}" % self.name

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                CameraTable.insert().values(
                    camera_name = self.name,
                    module = self.module
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        with connection.begin() as transaction:
            s = CameraTable.select().where(CameraTable.c.camera_name == cameraName)
            result = connection.execute(s).fetchone()
            return Camera(result['camera_name'], result['module'])


class AbstractFilter(DataUnit):

    dependencies = ()

    __slots__ = "_name"

    def __init__(self, name):
        assert isinstance(name, str)
        self._name = name

    def __hash__(self):
        return hash((self._name, ))

    def __eq__(self, other):
        return self._name == other._name

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self.name

    @property
    def pkey(self):
        return self.value

    def __repr__(self):
        return "{'AbstractFilter': '%s'}" % self.name

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                AbstractFilterTable.insert().values(
                    abstract_filter_name = self.name
                )
            )

    @staticmethod
    def find(values, connection):
        abstractFilterName = values['AbstractFilter']
        with connection.begin() as transaction:
            s = AbstractFilterTable.select().where(AbstractFilterTable.c.abstract_filter_name == abstractFilterName)
            result = connection.execute(s).fetchone()
            return AbstractFilter(result['abstract_filter_name'])


class PhysicalFilter(DataUnit):

    dependencies = (AbstractFilter, Camera)

    __slots__ = "_abstract", "_camera", "_name"

    def __init__(self, abstract, camera, name):
        assert isinstance(abstract, AbstractFilter) or abstract is None
        self._abstract = abstract
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(name, str)
        self._name = name

    def __hash__(self):
        return hash((self._abstract, self._camera, self._name))

    def __eq__(self, other):
        return self._abstract == other._abstract and self._camera == other._camera and self._name == other._name

    @property
    def abstract(self):
        return self._abstract

    @property
    def camera(self):
        return self._camera

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self.name

    @property
    def pkey(self):
        return (self.camera.value, self.value)

    def __repr__(self):
        return (
            "{'PhysicalFilter': '%s', 'Camera': '%s'}"
            % (self.name, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                PhysicalFilterTable.insert().values(
                    physical_filter_name = self.name,
                    camera_name = self.camera.name,
                    abstract_filter_name = self.abstract.name
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        physicalFilterName = values['PhysicalFilter']
        with connection.begin() as transaction:
            # First check if PhysicalFilter, specified by combined key, exists
            s = select([PhysicalFilterTable.c.abstract_filter_name]).where(and_(
                PhysicalFilterTable.c.camera_name == cameraName, PhysicalFilterTable.c.physical_filter_name == physicalFilterName))
            abstractFilterName = connection.execute(s).scalar()
            # Then load its components and construct the object
            abstractFilter = AbstractFilter.find(values, connection)
            camera = Camera.find(values, connection)
            return PhysicalFilter(abstractFilter, camera, physicalFilterName)


class PhysicalSensor(DataUnit):

    dependencies = (Camera,)

    __slots__ = "_camera", "_number", "_name", "_group", "_purpose"

    def __init__(self, camera, number, name, group, purpose):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(number, int)
        self._number = number
        assert isinstance(name, str)
        self._name = name
        assert isinstance(group, str)
        self._group = group
        assert isinstance(purpose, str)
        self._purpose = purpose

    def __hash__(self):
        return hash((self._camera, self._number, self._name, self._group, self._purpose))

    def __eq__(self, other):
        return self._camera == other._camera and self._number == other._number and self._group == other._group and self._purpose == other._purpose

    @property
    def camera(self):
        return self._camera

    @property
    def number(self):
        return self._number

    @property
    def name(self):
        return self._name

    @property
    def group(self):
        return self._group

    @property
    def purpose(self):
        return self._purpose

    @property
    def value(self):
        return self.number

    @property
    def pkey(self):
        return (self.camera.value, self.value)

    def __repr__(self):
        return (
            "{'PhysicalSensor': %d, 'Camera': '%s'}"
            % (self.number, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                PhysicalSensorTable.insert().values(
                    camera_name = self.camera.name,
                    physical_sensor_number = self.number,
                    name = self.name,
                    group = self.group,
                    purpose = self.purpose
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        physicalSensorNumber = values['PhysicalSensor']

        with connection.begin() as transaction:
            # First check if PhysicalSensor, specified by combined key, exists
            s = select([PhysicalSensorTable.c.name, PhysicalSensorTable.c.group, PhysicalSensorTable.c.purpose]).where(and_(
                PhysicalSensorTable.c.camera_name == cameraName, PhysicalSensorTable.c.physical_sensor_number == physicalSensorNumber))
            result = connection.execute(s).first()
            name = result[PhysicalSensorTable.c.name]
            group = result[PhysicalSensorTable.c.group]
            purpose = result[PhysicalSensorTable.c.purpose]
            # Then load its components and construct the object
            camera = Camera.find(values, connection)
            return PhysicalSensor(camera, physicalSensorNumber, name, group, purpose)


class Visit(DataUnit):

    dependencies = (Camera, PhysicalFilter)

    __slots__ = ("_camera", "_filter", "_number",
                 "_obsBegin", "_exposureTime", "region")

    def __init__(self, camera, number, filter, obsBegin, exposureTime, region=None):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(number, int)
        self._number = number
        assert isinstance(filter, PhysicalFilter)
        self._filter = filter
        assert isinstance(obsBegin, datetime)
        self._obsBegin = obsBegin
        assert isinstance(exposureTime, float)
        self._exposureTime = exposureTime
        self.region = region

    def __hash__(self):
        return hash((self._camera, self._filter, self._number, self._obsBegin, self._exposureTime, self.region))

    def __eq__(self, other):
        return self._camera == other._camera and self._filter == other._filter and self._number == other._number and self._obsBegin == other._obsBegin and self._exposureTime == other._exposureTime and self.region == other.region

    @property
    def camera(self):
        return self._camera

    @property
    def number(self):
        return self._number

    @property
    def filter(self):
        return self._filter

    @property
    def obsBegin(self):
        return self._obsBegin

    @property
    def exposureTime(self):
        return self._exposureTime

    @property
    def value(self):
        return self.number

    @property
    def pkey(self):
        return (self.camera.value, self.value)

    def __repr__(self):
        return (
            "{'Visit': %d, 'Camera': '%s'}"
            % (self.number, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                VisitTable.insert().values(
                    visit_number = self.number,
                    camera_name = self.camera.name,
                    physical_filter_name = self.filter.name,
                    obs_begin = self.obsBegin,
                    exposure_time = self.exposureTime,
                    region = self.region
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        visitNumber = values['Visit']

        with connection.begin() as transaction:
            s = VisitTable.select().where(and_(VisitTable.c.camera_name == cameraName, VisitTable.c.visit_number == visitNumber))
            result = connection.execute(s).fetchone()

            physicalFilterName = result[VisitTable.c.physical_filter_name]
            obsBegin = result[VisitTable.c.obs_begin]
            exposureTime = result[VisitTable.c.exposure_time]
            region = result[VisitTable.c.region]

            # Read child DataUnits
            camera = Camera.find(values, connection)
            physicalFilter = PhysicalFilter.find(values, connection)

            return Visit(camera, visitNumber, physicalFilter, obsBegin, exposureTime, region)


class ObservedSensor(DataUnit):

    dependencies = (Camera, PhysicalSensor, Visit)

    __slots__ = ("_camera", "_visit", "region")

    def __init__(self, camera, visit, physical, region):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(visit, Visit)
        self._visit = visit
        assert isinstance(physical, PhysicalSensor)
        self._physical = physical
        self.region = region

    def __hash__(self):
        return hash((_camera, _visit, region))

    def __eq__(self, other):
        return self._camera == other._camera and self._visit == other._visit and self.region == other.region

    @property
    def camera(self):
        return self._camera

    @property
    def visit(self):
        return self._visit

    @property
    def physical(self):
        return self._physical

    @property
    def value(self):
        return None

    @property
    def pkey(self):
        return (self.camera.value, self.visit.value, self.physical.value)

    def __repr__(self):
        return (
            "{'Visit': %d, 'PhysicalSensor': %d, 'Camera': '%s'}"
            % (self.visit.number, self.physical.number, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                ObservedSensorTable.insert().values(
                    visit_number = self.visit.number,
                    physical_sensor_number = self.physical.number,
                    camera_name = self.camera.name,
                    region = self.region
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        visitNumber = values['Visit']
        physicalSensorNumber = values['PhysicalSensor']

        with connection.begin() as transaction:
            s = ObservedSensorTable.select().where(and_(ObservedSensorTable.c.camera_name == cameraName, ObservedSensorTable.c.visit_number ==
                                                        visitNumber, ObservedSensorTable.c.physical_sensor_number == physicalSensorNumber))
            result = connection.execute(s).fetchone()

            physical_sensor_number = result[ObservedSensorTable.c.physical_sensor_number]
            region = result[ObservedSensorTable.c.region]

            # Read child DataUnits
            camera = Camera.find(values, connection)
            physicalSensor = PhysicalSensor.find(values, connection)
            visit = Visit.find(values, connection)

            return ObservedSensor(camera, visit, physicalSensor, region)


class Snap(DataUnit):

    dependencies = (Camera, Visit)

    __slots__ = ("_camera", "_visit", "_index", "_obsBegin", "_exposureTime")

    def __init__(self, camera, visit, index, obsBegin, exposureTime):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(visit, Visit)
        self._visit = visit
        assert isinstance(index, int)
        self._index = index
        assert isinstance(obsBegin, datetime)
        self._obsBegin = obsBegin
        assert isinstance(exposureTime, float)
        self._exposureTime = exposureTime

    def __hash__(self):
        return hash((self._camera, self._visit, self._index, self._obsBegin, self._exposureTime))

    def __eq__(self, other):
        return self._camera == other._camera and self._visit == other._visit and self._index == other._index and self._obsBegin == other._obsBegin and self._exposureTime == other._exposureTime

    @property
    def camera(self):
        return self._camera

    @property
    def visit(self):
        return self._visit

    @property
    def index(self):
        return self._index

    @property
    def obsBegin(self):
        return self._obsBegin

    @property
    def exposureTime(self):
        return self._exposureTime

    @property
    def value(self):
        return self.index

    @property
    def pkey(self):
        return (self.camera.value, self.visit.value, self.value)

    def __repr__(self):
        return (
            "{'Snap': %d, 'Visit': %d, 'Camera': '%s'}"
            % (self.index, self.visit.number, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                SnapTable.insert().values(
                    visit_number = self.visit.number,
                    snap_index = self.index,
                    camera_name = self.camera.name,
                    obs_begin = self.obsBegin,
                    exposure_time = self.exposureTime
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        visitNumber = values['Visit']
        snapIndex = values['Snap']

        with connection.begin() as transaction:
            s = SnapTable.select().where(and_(SnapTable.c.camera_name == cameraName,
                                              SnapTable.c.visit_number == visitNumber, SnapTable.c.snap_index == snapIndex))
            result = connection.execute(s).fetchone()

            obsBegin = result[SnapTable.c.obs_begin]
            exposureTime = result[SnapTable.c.exposure_time]

            camera = Camera.find(values, connection)
            visit = Visit.find(values, connection)

            return Snap(camera, visit, snapIndex, obsBegin, exposureTime)


class VisitRange(DataUnit):

    dependencies = (Camera,)

    __slots__ = ("_camera", "_visitBegin", "_visitEnd")

    def __init__(self, camera, visitBegin, visitEnd):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(visitBegin, int)
        self._visitBegin = visitBegin
        assert isinstance(visitEnd, int) or visitEnd is None
        self._visitEnd = visitEnd

    def __hash__(self):
        return hash((_camera, _visitBegin, _visitEnd))

    def __eq__(self, other):
        return self._camera == other.camera and self._visitBegin == other._visitBegin and self._visitEnd == other._visitEnd

    @property
    def camera(self):
        return self._camera

    @property
    def visitBegin(self):
        return self._visitBegin

    @property
    def visitEnd(self):
        return self._visitEnd

    @property
    def value(self):
        return (self.visitBegin, self.visitEnd)

    @property
    def pkey(self):
        return (self.camera.value,) + self.value

    def __repr__(self):
        return (
            "{'VisitRange': (%d, %d), 'Camera': '%s'}"
            % (self.visitBegin, self.visitEnd, self.camera.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                VisitRangeTable.insert().values(
                    visit_begin = self.visitBegin,
                    visit_end = self.visitEnd,
                    camera_name = self.camera.name
                )
            )

    @staticmethod
    def find(values, connection):
        cameraName = values['Camera']
        visitBegin, visitEnd = values['VisitRange']

        with connection.begin() as transaction:
            result = connection.execute(select([exists().where(and_(
                VisitRangeTable.c.camera_name == cameraName, VisitRangeTable.c.visit_begin == visitBegin, VisitRangeTable.c.visit_end == visitEnd))]))
            if result.scalar():
                camera = Camera.find(values, connection)
                return VisitRange(camera, visitBegin, visitEnd)
            else:
                raise ValueError("Visit range not found")


class SkyMap(DataUnit):

    dependencies = ()

    __slots__ = ("_name", "_module")

    def __init__(self, name, module=""):
        assert isinstance(name, str)
        assert isinstance(module, str)
        self._name = name
        self._module = module

    def __hash__(self):
        return hash((_name, _module))

    def __eq__(self, other):
        return self._name == other._name and self._module == other._module

    @property
    def name(self):
        return self._name

    @property
    def module(self):
        return self._module

    @property
    def value(self):
        return self._name

    @property
    def pkey(self):
        return self.value

    def makeTracts(self):
        raise NotImplementedError("pure virtual")

    @classmethod
    def deserialize(cls, name, blob):
        raise NotImplementedError("pure virtual")

    def __repr__(self):
        return ("{'SkyMap': '%s'}" % self.name)

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                SkyMapTable.insert().values(
                    skymap_name = self.name,
                    module = self.module,
                    serialized = bytes()
                )
            )

    @staticmethod
    def find(values, connection):
        skymapName = values['SkyMap']
        with connection.begin() as transaction:
            s = SkyMapTable.select().where(SkyMapTable.c.skymap_name == skymapName)
            result = connection.execute(s).fetchone()
            return SkyMap(result['skymap_name'], result['module'])


class Tract(DataUnit):

    dependencies = (SkyMap,)

    __slots__ = ("_skymap", "_number", "region")

    def __init__(self, skymap, number, region=None):
        assert isinstance(skymap, SkyMap)
        self._skymap = skymap
        assert isinstance(number, int)
        self._number = number
        self.region = region

    def __hash__(self):
        return hash((_skymap, _number, region))

    def __eq__(self, other):
        return self._skymap == other._skymap and self._number == other._number and self.region == other.region

    @property
    def skymap(self):
        return self._skymap

    @property
    def number(self):
        return self._number

    @property
    def value(self):
        return self.number

    @property
    def pkey(self):
        return (self.skymap.value, self.value)

    def __repr__(self):
        return (
            "{'Tract': %d, 'SkyMap': '%s'}"
            % (self.number, self.skymap.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                TractTable.insert().values(
                    tract_number = self.number,
                    skymap_name = self.skymap.name,
                    region = self.region
                )
            )

    @staticmethod
    def find(values, connection):
        skymapName = values['SkyMap']
        tractNumber = values['Tract']
        with connection.begin() as transaction:
            s = TractTable.select().where(and_(TractTable.c.skymap_name == skymapName, TractTable.c.tract_number == tractNumber))
            result = connection.execute(s).fetchone()
            region = result[TractTable.c.region]
            skymap = SkyMap.find(values, connection)
            return Tract(skymap, tractNumber, region)


class Patch(DataUnit):

    dependencies = (SkyMap, Tract)

    __slots__ = ("_skymap", "_tract", "_index", "_cellX", "_cellY", "region")

    def __init__(self, skymap, tract, index, cellX, cellY, region=None):
        assert isinstance(skymap, SkyMap)
        self._skymap = skymap
        assert isinstance(tract, Tract)
        self._tract = tract
        assert isinstance(index, int)
        self._index = index
        assert isinstance(cellX, int)
        self._cellX = cellX
        assert isinstance(cellY, int)
        self._cellY = cellY
        self.region = region

    def __hash__(self):
        return hash((_skymap, _tract, _index, _cellX, _cellY, region))

    def __eq__(self, other):
        return self._skymap == other._skymap and self._tract == other._tract and self._index == other._index and self._cellX == other._cellX and self._cellY == other._cellY and self.region == other.region

    @property
    def skymap(self):
        return self._skymap

    @property
    def tract(self):
        return self._tract

    @property
    def index(self):
        return self._index

    @property
    def cellX(self):
        return self._cellX

    @property
    def cellY(self):
        return self._cellY

    @property
    def value(self):
        return self.index

    @property
    def pkey(self):
        return (self.skymap.value, self.tract.value, self.value)

    def __repr__(self):
        return (
            "{'Patch': %d, 'Tract': %d, 'SkyMap': '%s'}"
            % (self.index, self.tract.number, self.skymap.name)
        )

    def insert(self, connection):
        with connection.begin() as transaction:
            connection.execute(
                PatchTable.insert().values(
                    patch_index = self.index,
                    tract_number = self.tract.number,
                    cell_x = self.cellX,
                    cell_y = self.cellY,
                    skymap_name = self.skymap.name,
                    region = self.region
                )
            )

    @staticmethod
    def find(values, connection):
        skymapName = values['SkyMap']
        tractNumber = values['Tract']
        patchIndex = values['Patch']
        with connection.begin() as transaction:
            s = PatchTable.select().where(and_(PatchTable.c.skymap_name == skymapName, PatchTable.c.patch_index ==
                                               patchIndex, PatchTable.c.tract_number == tractNumber))
            result = connection.execute(s).fetchone()
            cellX = result[PatchTable.c.cell_x]
            cellY = result[PatchTable.c.cell_y]
            region = result[PatchTable.c.region]
            skymap = SkyMap.find(values, connection)
            tract = Tract.find(values, connection)
            return Patch(skymap, tract, patchIndex, cellX, cellY, region)
