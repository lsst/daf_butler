from types import MappingProxyType


class DatasetType:

    __slots__ = "name", "template", "units", "StorageClass"

    def __init__(self, name, template, units, storageClass):
        self.name = name
        self.template = template
        self.units = units
        self.storageClass = storageClass


class DatasetLabel:

    __slots__ = "_name", "_values"

    def __init__(self, name, **values):
        self._name = name
        self._values = values

    @property
    def name(self):
        return self._name

    def __eq__(self, other):
        if not isinstance(DatasetLabel, other):
            return NotImplemented
        return self._name == other._name and self._values == other._values

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self._name, self._values.items())


class DatasetRef(DatasetLabel):

    __slots__ = ("_type", "_units", "_producer",
                 "_predictedConsumers", "_actualConsumers")

    def __init__(self, type, units):
        units = type.units.conform(units)
        super().__init__(
            type.name,
            **{name: unit.value for name, unit in units.items()}
        )
        self._type = type
        self._units = MappingProxyType(units)
        self._producer = None
        self._predictedConsumers = dict()
        self._actualConsumers = dict()

    @property
    def type(self):
        return self._type

    @property
    def units(self):
        return self._units

    @property
    def producer(self):
        return self._producer

    @property
    def predictedConsumers(self):
        return MappingProxyType(self._predictedConsumers)

    @property
    def actualConsumers(self):
        return MappingProxyType(self._actualConsumers)

    def makePath(self, run, template=None):
        raise NotImplementedError("TODO")


class DatasetHandle(DatasetRef):

    __slots__ = "_uri", "_components", "_run"

    def __init__(self, ref, uri, components, run):
        super().__init__(ref.type, ref.units)
        self._producer = ref.producer
        self._predictedConsumers.update(ref.predictedConsumers)
        self._actualConsumers.update(ref.actualConsumers)
        self._uri = uri
        self._components = MappingProxyType(components)
        self._run = run

    @property
    def uri(self):
        return self._uri

    @property
    def components(self):
        return self._components

    @property
    def run(self):
        return self._run
