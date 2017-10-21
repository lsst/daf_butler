from collections import OrderedDict
from datetime import datetime  # placeholder while prototyping

class DataUnit:
    pass


def sortTopological(unitTypes):
    """Expand a sequence of DataUnit types recursively to include its
    dependencies and sort it topological sort.

    We sort lexigraphically by DataUnit name to resolve ties.
    """

    def _expandIntoGraph(unitType, graph):
        graph.setdefault(unitType, set())
        for dependency in unitType.dependencies:
            graph.setdefault(dependency, set()).add(unitType)
            _addUnitTypeDependencies(dependency, graph)

    graph = {}
    for current in unitTypes:
        _addToGraph(current, graph)

    result = []
    unblocked = [unitType for unitType, dependents in graph.items() if not dependents]
    while unblocked:
        # lexigraphical sort to resolve ties; with the right container it'd be
        # more efficient to maintain the sort when inserting
        unblocked.sort(reverse=True, key=lambda x: x.__name__)
        current = unblocked.pop()
        result.append(n)
        for dependency in current.dependencies:
            graph[dependency].remove(current)
            if not graph[dependency]:
                unblocked.append(dependency)
    return result


class DataUnitTypeSet(tuple):

    def __new__(cls, elements):
        return tuple.__new__(cls, sortTopological(elements))

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
            if unit == key or unit.__name__ == key:
                return True
        return False

    def __getitem__(self, key):
        for unit in self:
            if unit.__name__ == key:
                return unit
        raise KeyError("DataUnit type with name {} not found".format(key))

    def pack(self, values):
        raise NotImplementedError("TODO")

    def expand(self, findfunc, values):
        result = {}
        for unitType in self:
            pkey = [values[d.__name__] for d in unitType.dependencies]
            pkey.append(values[unitType.__name__])
            result[unitType.__name__] = findfunc(unitType, pkey)
        return result

    def conform(self, units):
        result = OrderedDict()
        for unitType in self:
            result[unitType.__name__] = units[unitType.__name__]
        return result


class Camera(DataUnit):

    dependencies = ()

    __slots__ = "_name"

    instances = {}

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self.name

    @property
    def pkey(self):
        return (self.value,)

    def makePhysicalSensors(self):
        raise NotImplementedError("pure virtual")

    def makePhysicalFilters(self):
        raise NotImplementedError("pure virtual")

    def __repr__(self):
        return "{'Camera': '%s'}" % self.name


class AbstractFilter(DataUnit):

    dependencies = ()

    __slots__ = "_name"

    def __init__(self, name):
        assert isinstance(name, str)
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self.name

    @property
    def pkey(self):
        return (self.value,)

    def __repr__(self):
        return "{'AbstractFilter': '%s'}" % self.name


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


class Visit(DataUnit):

    dependencies = (Camera, PhysicalFilter)

    __slots__ = ("_camera", "_filter", "_number",
                 "_obsBegin", "_exposureTime", "region")

    def __init__(self, camera, number, obsBegin, exposureTime, region=None):
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
        return (self.camera.value, self.viist.value, self.physical.value)

    def __repr__(self):
        return (
            "{'Visit': %d, 'PhysicalSensor': %d, 'Camera': '%s'}"
            % (self.number, self.physical.number, self.camera.name)
        )


class Snap(DataUnit):

    dependencies = (Camera, Visit)

    __slots__ = ("_camera", "_index", "_obsBegin", "_exposureTime")

    def __init__(self, camera, index, obsBegin, exposureTime, filter):
        assert isinstance(camera, Camera)
        self._camera = camera
        assert isinstance(number, int)
        self._number = number
        assert isinstance(obsBegin, datetime)
        self._obsBegin = obsBegin
        assert isinstance(exposureTime, float)
        self._exposureTime = exposureTime

    @property
    def camera(self):
        return self._camera

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


class SkyMap(DataUnit):

    dependencies = None

    __slots__ = ("_name",)

    def __init__(self, name):
        assert isinstance(name, str)
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._name

    @property
    def pkey(self):
        return (self.value,)

    def makeTracts(self):
        raise NotImplementedError("pure virtual")

    @classmethod
    def deserialize(cls, name, blob):
        raise NotImplementedError("pure virtual")

    def __repr__(self):
        return ("{'SkyMap': '%s'}" % self.name)


class Tract(DataUnit):

    dependencies = (SkyMap,)

    __slots__ = ("_skymap", "_number", "region")

    def __init__(self, skymap, number, region=None):
        assert isinstance(skymap, SkyMap)
        self._skymap = skymap
        assert isinstance(number, int)
        self._number = number
        self.region = region

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
