from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime, ForeignKey, ForeignKeyConstraint, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

MasterCalibVisitJoin = Table('MasterCalibVisitJoin', Base.metadata,
    Column('visit_begin', Integer, nullable=False),
    Column('visit_end', Integer, nullable=False),
    Column('physical_filter_name', String, nullable=False),
    Column('visit_number', String, nullable=False),
    Column('camera_name', String, nullable=False),
    ForeignKeyConstraint(['visit_begin', 'visit_end', 'physical_filter_name', 'camera_name'], ['MasterCalib.visit_begin', 'MasterCalib.visit_end', 'MasterCalib.physical_filter_name', 'MasterCalib.camera_name']),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
)

SensorPatchJoin = Table('SensorPatchJoin', Base.metadata,
    Column('visit_number', Integer, nullable=False),
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('patch_index', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], ['ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
    ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], ['Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
)

SensorTractJoin = Table('SensorTractJoin', Base.metadata,
    Column('visit_number', Integer, nullable=False),
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], ['ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Patch.tract_number', 'Patch.skymap_name'])
)

VisitPatchJoin = Table('VisitPatchJoin', Base.metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('patch_index', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], ['Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
)

VisitTractJoin = Table('VisitTractJoin', Base.metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name'])
)

PhysicalFilterDatasetJoins = Table('PhysicalFilterDatasetJoins', Base.metadata,
    Column('physical_filter_name', String, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['physical_filter_name', 'camera_name'], ['PhysicalFilter.physical_filter_name', 'PhysicalFilter.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

PhysicalSensorDatasetJoin = Table('PhysicalSensorDatasetJoin', Base.metadata,
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['physical_sensor_number', 'camera_name'], ['PhysicalSensor.physical_sensor_number', 'PhysicalSensor.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

VisitDatasetJoin = Table('VisitDatasetJoin', Base.metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

SnapDatasetJoin = Table('SnapDatasetJoin', Base.metadata,
    Column('snap_index', Integer, nullable=False),
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['snap_index', 'visit_number', 'camera_name'], ['Snap.snap_index', 'Snap.visit_number', 'Snap.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

AbstractFilterDatasetJoin = Table('AbstractFilterDatasetJoin', Base.metadata,
    Column('abstract_filter_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['abstract_filter_name'], ['AbstractFilter.abstract_filter_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

TractDatasetJoin = Table('TractDatasetJoin', Base.metadata,
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

PatchDatasetJoin = Table('PatchDatasetJoin', Base.metadata,
    Column('patch_index', Integer, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['patch_index', 'tract_number', 'skymap_name'], ['Patch.patch_index', 'Patch.tract_number', 'Patch.skymap_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

class Dataset(Base):
    __tablename__ = 'Dataset'
    dataset_id = Column(Integer, primary_key=True, nullable=False)
    registry_id = Column(Integer, primary_key=True, nullable=False)
    dataset_type_name = Column(Integer, ForeignKey('DatasetType.name'), nullable=False)
    unit_pack = Column(LargeBinary, nullable=False)
    uri = Column(String)
    run_id = Column(Integer, nullable=False)
    producer_id = Column(Integer)
    ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id'])
    ForeignKeyConstraint(['producer_id', 'registry_id'], ['Quantum.producer_id', 'Quantum.registry_id'])
    physical_sensors = relationship(
        "PhysicalSensor",
        secondary=PhysicalSensorDatasetJoin,
        backref="datasets")
    visits = relationship(
        "Visits",
        secondary=VisitDatasetJoin,
        backref="datasets")
    snaps = relationship(
        "Snaps",
        secondary=SnapDatasetJoin,
        backref="datasets")
    abstract_filters = relationship(
        "AbstractFilters",
        secondary=AbstractFilterDatasetJoin,
        backref="datasets")
    tracts = relationship(
        "Tracts",
        secondary=TractDatasetJoin,
        backref="datasets")
    patches = relationship(
        "Patches",
        secondary=PatchDatasetJoin,
        backref="datasets")

class DatasetComposition(Base):
    __tablename__ = 'DatasetComposition'
    parent_dataset_id = Column(Integer, primary_key=True, nullable=False)
    parent_registry_id = Column(Integer, primary_key=True, nullable=False)
    component_dataset_id = Column(Integer, primary_key=True, nullable=False)
    component_registry_id = Column(Integer, primary_key=True, nullable=False)
    component_name = Column(String, nullable=False)
    ForeignKeyConstraint(['parent_dataset_id', 'parent_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
    ForeignKeyConstraint(['component_dataset_id', 'component_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])

class DatasetType(Base):
    __tablename__ = 'DatasetType'
    name = Column(String, primary_key=True, nullable=False)
    template = Column(String)
    storage_class = Column(String, nullable=False)


class DatasetTypeUnits(Base):
    __tablename__ = 'DatasetTypeUnits'
    dataset_type_name = Column(String, ForeignKey('DatasetType.name'), primary_key=True, nullable=False)
    unit_name = Column(String, nullable=False)

class DatasetCollections(Base):
    __tablename__ = 'DatasetCollections'
    tag = Column(String, nullable=False)
    dataset_id = Column(Integer, primary_key=True, nullable=False)
    registry_id = Column(Integer, primary_key=True, nullable=False)
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])

class Run(Base):
    __tablename__ = 'Run'
    run_id = Column(Integer, primary_key=True, nullable=False)
    registry_id = Column(Integer, primary_key=True, nullable=False)
    tag	= Column(String)
    environment_id = Column(Integer)
    pipeline_id = Column(Integer)
    ForeignKeyConstraint(['environment_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
    ForeignKeyConstraint(['pipeline_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])

class Quantum(Base):
    __tablename__ = 'Quantum'
    quantum_id = Column(Integer, primary_key=True, nullable=False)
    registry_id = Column(Integer, primary_key=True, nullable=False)
    run_id = Column(Integer, nullable=False)
    task = Column(String)
    ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id'])

class DatasetConsumers(Base):
    __tablename__ = 'DatasetConsumers'
    quantum_id = Column(Integer, ForeignKey('Quantum.quantum_id'), primary_key=True, nullable=False)
    quantum_registry_id = Column(Integer, ForeignKey('Quantum.registry_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer, ForeignKey('Dataset.dataset_id'), primary_key=True, nullable=False)
    dataset_registry_id = Column(Integer, ForeignKey('Dataset.registry_id'), primary_key=True, nullable=False)
    actual = Column(Boolean, nullable=False)
    ForeignKeyConstraint(['quantum_id', 'quantum_registry_id'], ['Quantum.quantum_id', 'Quantum.registry_id'])
    ForeignKeyConstraint(['dataset_id', 'dataset_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])

class AbstractFilter(Base):
    __tablename__ = 'AbstractFilter'
    abstract_filter_name = Column(String, primary_key=True, nullable=False)

class Camera(Base):
    __tablename__ = 'Camera'
    camera_name = Column(String, primary_key=True, nullable=False)
    module = Column(String, primary_key=True, nullable=False)

class PhysicalFilter(Base):
    __tablename__ = 'PhysicalFilter'
    physical_filter_name = Column(String, primary_key=True, nullable=False)
    camera_name = Column(String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
    abstract_filter_name = ForeignKey('AbstractFilter.abstract_filter_name'), Column(String)

class PhysicalSensor(Base):
    __tablename__ = 'PhysicalSensor'
    physical_sensor_number = Column(String, primary_key=True, nullable=False)
    name = Column(String, primary_key=True, nullable=False)
    camera_name = Column(String, ForeignKey('Camera.camera_name'), nullable=False)
    group = Column(String)
    purpose = Column(String)
    patches = relationship(
        "Patch",
        secondary=SensorPatchJoin,
        back_populates="physical_sensors")
    tracts = relationship(
        "PhysicalSensor",
        secondary=SensorTractJoin,
        back_populates="physical_sensors")

class Visit(Base):
    __tablename__ = 'Visit'
    visit_number = Column(Integer, primary_key=True, nullable=False)
    camera_name = Column(String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
    physical_filter_name = Column(String, ForeignKey('PhysicalFilter.physical_filter_name'), nullable=False)
    obs_begin = Column(DateTime)
    obs_end = Column(DateTime)
    region = Column(LargeBinary)
    master_calibs = relationship(
        "Visit",
        secondary=MasterCalibVisitJoin,
        back_populates="visits")
    patches = relationship(
        "Patch",
        secondary=VisitPatchJoin,
        back_populates="visits")
    tracts = relationship(
        "Tract",
        secondary=VisitTractJoin,
        back_populates="visits")

class ObservedSensor(Base):
    __tablename__ = 'ObservedSensor'
    visit_number = Column(Integer, ForeignKey('Visit.visit_number'), primary_key=True, nullable=False)
    physical_sensor_number = Column(Integer, ForeignKey('PhysicalSensor.physical_sensor_number'), primary_key=True, nullable=False)
    camera_name	= Column(String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
    region = Column(LargeBinary)

class Snap(Base):
    __tablename__ = 'Snap'
    visit_number = Column(Integer, primary_key=True, nullable=False)
    snap_index = Column(Integer, primary_key=True, nullable=False)
    camera_name	= Column(String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
    obs_begin = Column(DateTime, nullable=False)
    obs_end	= Column(DateTime, nullable=False)
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name'])

class MasterCalib(Base):
    __tablename__ = 'MasterCalib'
    visit_begin = Column(Integer)
    visit_end = Column(Integer)
    physical_filter_name = Column(String, nullable=False)
    camera_name	= Column(String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
    ForeignKeyConstraint(['physical_filter_name', 'camera_name'], ['PhysicalFilter.physical_filter_name', 'PhysicalFilter.camera_name'])
    visits = relationship(
        "Visit",
        secondary=MasterCalibVisitJoin,
        back_populates="master_calibs")

class SkyMap(Base):
    __tablename__ = 'SkyMap'
    skymap_name = Column(String, primary_key=True, nullable=False)
    module = Column(String, nullable=False)
    serialized = Column(LargeBinary, nullable=False)

class Tract(Base):
    __tablename__ = 'Tract'
    tract_number = Column(Integer, primary_key=True, nullable=False)
    skymap_name	= Column(String, ForeignKey('SkyMap.skymap_name'), primary_key=True, nullable=False)
    region = Column(LargeBinary)
    physical_sensors = relationship(
        "PhysicalSensor",
        secondary=SensorTractJoin,
        back_populates="tracts")
    visits = relationship(
        "Visit",
        secondary=VisitTractJoin,
        back_populates="tracts")

class Patch(Base):
    __tablename__ = 'Patch'
    patch_index = Column(Integer, primary_key=True, nullable=False)
    tract_number = Column(Integer, primary_key=True, nullable=False)
    skymap_name = Column(String, ForeignKey('SkyMap.skymap_name'), primary_key=True, nullable=False)
    region = Column(LargeBinary)
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name'])
    physical_sensors = relationship(
        "PhysicalSensor",
        secondary=SensorPatchJoin,
        back_populates="patches")
    visits = relationship(
        "Visit",
        secondary=VisitPatchJoin,
        back_populates="patches")


from sqlalchemy import create_engine
engine = create_engine('sqlite:///:memory:')

from sqlalchemy.orm import sessionmaker
session = sessionmaker()
session.configure(bind=engine)
Base.metadata.create_all(engine)