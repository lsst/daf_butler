from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime, Float, ForeignKey, ForeignKeyConstraint, Table, MetaData

metadata = MetaData()

Dataset = Table('Dataset', metadata,
    Column('dataset_id', Integer, primary_key=True, nullable=False),
    Column('registry_id', Integer, primary_key=True, nullable=False),
    Column('dataset_type_name', Integer, ForeignKey('DatasetType.dataset_type_name'), nullable=False),
    Column('unit_pack', LargeBinary, nullable=False),
    Column('uri', String),
    Column('run_id', Integer, nullable=False),
    Column('producer_id', Integer),
    ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id']),
    ForeignKeyConstraint(['producer_id', 'registry_id'], ['Quantum.quantum_id', 'Quantum.registry_id'])
)

DatasetComposition = Table('DatasetComposition', metadata,
    Column('parent_dataset_id', Integer, primary_key=True, nullable=False),
    Column('parent_registry_id', Integer, primary_key=True, nullable=False),
    Column('component_dataset_id', Integer, primary_key=True, nullable=False),
    Column('component_registry_id', Integer, primary_key=True, nullable=False),
    Column('component_name', String, nullable=False),
    ForeignKeyConstraint(['parent_dataset_id', 'parent_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id']),
    ForeignKeyConstraint(['component_dataset_id', 'component_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

DatasetType = Table('DatasetType', metadata,
    Column('dataset_type_name', String, primary_key=True, nullable=False),
    Column('template', String),
    Column('storage_class', String, nullable=False)
)

DatasetTypeUnits = Table('DatasetTypeUnits', metadata,
    Column('dataset_type_name', String, ForeignKey('DatasetType.dataset_type_name'), primary_key=True, nullable=False),
    Column('unit_name', String, primary_key=True, nullable=False)
)

DatasetCollections = Table('DatasetCollections', metadata,
    Column('tag', String, nullable=False),
    Column('dataset_id', Integer, primary_key=True, nullable=False),
    Column('registry_id', Integer, primary_key=True, nullable=False),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

Run = Table('Run', metadata,
    Column('run_id', Integer, primary_key=True, nullable=False),
    Column('registry_id', Integer, primary_key=True, nullable=False),
    Column('tag', String),
    Column('environment_id', Integer),
    Column('pipeline_id', Integer),
    ForeignKeyConstraint(['environment_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id']),
    ForeignKeyConstraint(['pipeline_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

Quantum = Table('Quantum', metadata,
    Column('quantum_id', Integer, primary_key=True, nullable=False),
    Column('registry_id', Integer, primary_key=True, nullable=False),
    Column('run_id', Integer, nullable=False),
    Column('task', String),
    ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id'])
)

DatasetConsumers = Table('DatasetConsumers', metadata,
    Column('quantum_id', Integer, ForeignKey('Quantum.quantum_id'), primary_key=True, nullable=False),
    Column('quantum_registry_id', Integer, ForeignKey('Quantum.registry_id'), primary_key=True, nullable=False),
    Column('dataset_id', Integer, ForeignKey('Dataset.dataset_id'), primary_key=True, nullable=False),
    Column('dataset_registry_id', Integer, ForeignKey('Dataset.registry_id'), primary_key=True, nullable=False),
    Column('actual', Boolean, nullable=False),
    ForeignKeyConstraint(['quantum_id', 'quantum_registry_id'], ['Quantum.quantum_id', 'Quantum.registry_id']),
    ForeignKeyConstraint(['dataset_id', 'dataset_registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

AbstractFilter = Table('AbstractFilter', metadata,
    Column('abstract_filter_name', String, primary_key=True, nullable=False)
)

Camera = Table('Camera', metadata,
    Column('camera_name', String, primary_key=True, nullable=False),
    Column('module', String, primary_key=True, nullable=False)
)

PhysicalFilter = Table('PhysicalFilter', metadata,
    Column('physical_filter_name', String, primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False),
    Column('abstract_filter_name', String, ForeignKey('AbstractFilter.abstract_filter_name'), nullable=True)
)

PhysicalSensor = Table('PhysicalSensor', metadata,
    Column('physical_sensor_number', String, primary_key=True, nullable=False),
    Column('name', String, primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), nullable=False),
    Column('group', String),
    Column('purpose', String)
)

Visit = Table('Visit', metadata,
    Column('visit_number', Integer, primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False),
    Column('physical_filter_name', String, ForeignKey('PhysicalFilter.physical_filter_name'), nullable=False),
    Column('obs_begin', DateTime, nullable=False),
    Column('exposure_time', Float, nullable=False),
    Column('region', LargeBinary)
)

ObservedSensor = Table('ObservedSensor', metadata,
    Column('visit_number', Integer, ForeignKey('Visit.visit_number'), primary_key=True, nullable=False),
    Column('physical_sensor_number', Integer, ForeignKey('PhysicalSensor.physical_sensor_number'), primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False),
    Column('region', LargeBinary)
)

Snap = Table('Snap', metadata,
    Column('visit_number', Integer, primary_key=True, nullable=False),
    Column('snap_index', Integer, primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False),
    Column('obs_begin', DateTime, nullable=False),
    Column('exposure_time', Float, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name'])
)

VisitRange = Table('VisitRange', metadata,
    Column('visit_begin', Integer, primary_key=True, nullable=False),
    Column('visit_end', Integer, primary_key=True, nullable=False),
    Column('camera_name', String, ForeignKey('Camera.camera_name'), primary_key=True, nullable=False)
)

SkyMap = Table('SkyMap', metadata,
    Column('skymap_name', String, primary_key=True, nullable=False),
    Column('module', String, nullable=False),
    Column('serialized', LargeBinary, nullable=False)
)

Tract = Table('Tract', metadata,
    Column('tract_number', Integer, primary_key=True, nullable=False),
    Column('skymap_name', String, ForeignKey('SkyMap.skymap_name'), primary_key=True, nullable=False),
    Column('region', LargeBinary)
)

Patch = Table('Patch', metadata,
    Column('patch_index', Integer, primary_key=True, nullable=False),
    Column('tract_number', Integer, primary_key=True, nullable=False),
    Column('skymap_name', String, ForeignKey('SkyMap.skymap_name'), primary_key=True, nullable=False),
    Column('region', LargeBinary),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name'])
)

SensorPatchJoin = Table('SensorPatchJoin', metadata,
    Column('visit_number', Integer, nullable=False),
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('patch_index', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], ['ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
    ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], ['Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
)

SensorTractJoin = Table('SensorTractJoin', metadata,
    Column('visit_number', Integer, nullable=False),
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], ['ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name'])
)

VisitPatchJoin = Table('VisitPatchJoin', metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('patch_index', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], ['Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
)

VisitTractJoin = Table('VisitTractJoin', metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name'])
)

PhysicalFilterDatasetJoins = Table('PhysicalFilterDatasetJoins', metadata,
    Column('physical_filter_name', String, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['physical_filter_name', 'camera_name'], ['PhysicalFilter.physical_filter_name', 'PhysicalFilter.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

PhysicalSensorDatasetJoin = Table('PhysicalSensorDatasetJoin', metadata,
    Column('physical_sensor_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['physical_sensor_number', 'camera_name'], ['PhysicalSensor.physical_sensor_number', 'PhysicalSensor.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

VisitDatasetJoin = Table('VisitDatasetJoin', metadata,
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['visit_number', 'camera_name'], ['Visit.visit_number', 'Visit.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

SnapDatasetJoin = Table('SnapDatasetJoin', metadata,
    Column('snap_index', Integer, nullable=False),
    Column('visit_number', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['snap_index', 'visit_number', 'camera_name'], ['Snap.snap_index', 'Snap.visit_number', 'Snap.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

VisitRangeDatasetJoin = Table('VisitRangeDatasetJoin', metadata,
    Column('visit_begin', Integer, nullable=False),
    Column('visit_end', Integer, nullable=False),
    Column('camera_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['visit_begin', 'visit_end', 'camera_name'], ['VisitRange.visit_begin', 'VisitRange.visit_end', 'VisitRange.camera_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

AbstractFilterDatasetJoin = Table('AbstractFilterDatasetJoin', metadata,
    Column('abstract_filter_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['abstract_filter_name'], ['AbstractFilter.abstract_filter_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

TractDatasetJoin = Table('TractDatasetJoin', metadata,
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['tract_number', 'skymap_name'], ['Tract.tract_number', 'Tract.skymap_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

PatchDatasetJoin = Table('PatchDatasetJoin', metadata,
    Column('patch_index', Integer, nullable=False),
    Column('tract_number', Integer, nullable=False),
    Column('skymap_name', String, nullable=False),
    Column('dataset_id', Integer, nullable=False),
    Column('registry_id', Integer, nullable=False),
    ForeignKeyConstraint(['patch_index', 'tract_number', 'skymap_name'], ['Patch.patch_index', 'Patch.tract_number', 'Patch.skymap_name']),
    ForeignKeyConstraint(['dataset_id', 'registry_id'], ['Dataset.dataset_id', 'Dataset.registry_id'])
)

