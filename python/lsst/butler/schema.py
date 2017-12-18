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

from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime, Float, ForeignKey, ForeignKeyConstraint, Table, MetaData

metadata = MetaData()

DatasetTable = \
    Table('Dataset', metadata,
          Column('dataset_id', Integer, primary_key=True, nullable=False),
          Column('registry_id', Integer, primary_key=True, nullable=False),
          Column('dataset_type_name', Integer, ForeignKey('DatasetType.dataset_type_name'), nullable=False),
          Column('uri', String),
          Column('run_id', Integer, nullable=False),
          Column('producer_id', Integer),
          Column('unit_hash', LargeBinary(512), nullable=False),
          ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id']),
          ForeignKeyConstraint(['producer_id', 'registry_id'], [
              'Quantum.quantum_id', 'Quantum.registry_id'])
          )

DatasetCompositionTable = \
    Table('DatasetComposition', metadata,
          Column('parent_dataset_id', Integer, primary_key=True, nullable=False),
          Column('parent_registry_id', Integer, primary_key=True, nullable=False),
          Column('component_dataset_id', Integer, primary_key=True, nullable=False),
          Column('component_registry_id', Integer, primary_key=True, nullable=False),
          Column('component_name', String, nullable=False),
          ForeignKeyConstraint(['parent_dataset_id', 'parent_registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id']),
          ForeignKeyConstraint(['component_dataset_id', 'component_registry_id'],
                               ['Dataset.dataset_id', 'Dataset.registry_id'])
          )

DatasetTypeTable = \
    Table('DatasetType', metadata,
          Column('dataset_type_name', String, primary_key=True, nullable=False),
          Column('template', String),
          Column('storage_class', String, nullable=False)
          )

DatasetTypeUnitsTable = \
    Table('DatasetTypeUnits', metadata,
          Column('dataset_type_name', String, ForeignKey(
              'DatasetType.dataset_type_name'), primary_key=True, nullable=False),
          Column('unit_name', String, primary_key=True, nullable=False)
          )

DatasetCollectionsTable = \
    Table('DatasetCollections', metadata,
          Column('tag', String, primary_key=True, nullable=False),
          Column('dataset_id', Integer, primary_key=True, nullable=False),
          Column('registry_id', Integer, primary_key=True, nullable=False),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

RunTable = \
    Table('Run', metadata,
          Column('run_id', Integer, primary_key=True, nullable=False),
          Column('registry_id', Integer, primary_key=True, nullable=False),
          Column('tag', String),
          Column('environment_id', Integer),
          Column('pipeline_id', Integer),
          ForeignKeyConstraint(['environment_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id']),
          ForeignKeyConstraint(['pipeline_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

QuantumTable = \
    Table('Quantum', metadata,
          Column('quantum_id', Integer, primary_key=True, nullable=False),
          Column('registry_id', Integer, primary_key=True, nullable=False),
          Column('run_id', Integer, nullable=False),
          Column('task', String),
          ForeignKeyConstraint(['run_id', 'registry_id'], ['Run.run_id', 'Run.registry_id'])
          )

DatasetConsumersTable = \
    Table('DatasetConsumers', metadata,
          Column('quantum_id', Integer, ForeignKey(
              'Quantum.quantum_id'), primary_key=True, nullable=False),
          Column('quantum_registry_id', Integer, ForeignKey(
              'Quantum.registry_id'), primary_key=True, nullable=False),
          Column('dataset_id', Integer, ForeignKey(
              'Dataset.dataset_id'), primary_key=True, nullable=False),
          Column('dataset_registry_id', Integer, ForeignKey(
              'Dataset.registry_id'), primary_key=True, nullable=False),
          Column('actual', Boolean, nullable=False),
          ForeignKeyConstraint(['quantum_id', 'quantum_registry_id'], [
              'Quantum.quantum_id', 'Quantum.registry_id']),
          ForeignKeyConstraint(['dataset_id', 'dataset_registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

AbstractFilterTable = \
    Table('AbstractFilter', metadata,
          Column('abstract_filter_name', String, primary_key=True, nullable=False)
          )

CameraTable = \
    Table('Camera', metadata,
          Column('camera_name', String, primary_key=True, nullable=False),
          Column('module', String, primary_key=True, nullable=False)
          )

PhysicalFilterTable = \
    Table('PhysicalFilter', metadata,
          Column('physical_filter_name', String, primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey(
              'Camera.camera_name'), primary_key=True, nullable=False),
          Column('abstract_filter_name', String, ForeignKey(
              'AbstractFilter.abstract_filter_name'), nullable=True)
          )

PhysicalSensorTable = \
    Table('PhysicalSensor', metadata,
          Column('physical_sensor_number', String, primary_key=True, nullable=False),
          Column('name', String, primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey('Camera.camera_name'), nullable=False),
          Column('group', String),
          Column('purpose', String)
          )

VisitTable = \
    Table('Visit', metadata,
          Column('visit_number', Integer, primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey(
              'Camera.camera_name'), primary_key=True, nullable=False),
          Column('physical_filter_name', String, ForeignKey(
              'PhysicalFilter.physical_filter_name'), primary_key=True, nullable=False),
          Column('obs_begin', DateTime, nullable=False),
          Column('exposure_time', Float, nullable=False),
          Column('region', LargeBinary)
          )

ObservedSensorTable = \
    Table('ObservedSensor', metadata,
          Column('visit_number', Integer, ForeignKey(
              'Visit.visit_number'), primary_key=True, nullable=False),
          Column('physical_sensor_number', Integer, ForeignKey(
              'PhysicalSensor.physical_sensor_number'), primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey(
              'Camera.camera_name'), primary_key=True, nullable=False),
          Column('region', LargeBinary)
          )

SnapTable = \
    Table('Snap', metadata,
          Column('visit_number', Integer, primary_key=True, nullable=False),
          Column('snap_index', Integer, primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey(
              'Camera.camera_name'), primary_key=True, nullable=False),
          Column('obs_begin', DateTime, nullable=False),
          Column('exposure_time', Float, nullable=False),
          ForeignKeyConstraint(['visit_number', 'camera_name'], [
              'Visit.visit_number', 'Visit.camera_name'])
          )

VisitRangeTable = \
    Table('VisitRange', metadata,
          Column('visit_begin', Integer, primary_key=True, nullable=False),
          Column('visit_end', Integer, primary_key=True, nullable=False),
          Column('camera_name', String, ForeignKey(
              'Camera.camera_name'), primary_key=True, nullable=False)
          )

SkyMapTable = \
    Table('SkyMap', metadata,
          Column('skymap_name', String, primary_key=True, nullable=False),
          Column('module', String, nullable=False),
          Column('serialized', LargeBinary, nullable=False)
          )

TractTable = \
    Table('Tract', metadata,
          Column('tract_number', Integer, primary_key=True, nullable=False),
          Column('skymap_name', String, ForeignKey(
              'SkyMap.skymap_name'), primary_key=True, nullable=False),
          Column('region', LargeBinary)
          )

PatchTable = \
    Table('Patch', metadata,
          Column('patch_index', Integer, primary_key=True, nullable=False),
          Column('tract_number', Integer, primary_key=True, nullable=False),
          Column('cell_x', Integer, nullable=False),
          Column('cell_y', Integer, nullable=False),
          Column('skymap_name', String, ForeignKey(
              'SkyMap.skymap_name'), primary_key=True, nullable=False),
          Column('region', LargeBinary),
          ForeignKeyConstraint(['tract_number', 'skymap_name'], [
              'Tract.tract_number', 'Tract.skymap_name'])
          )

SensorPatchJoinTable = \
    Table('SensorPatchJoin', metadata,
          Column('visit_number', Integer, nullable=False),
          Column('physical_sensor_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('tract_number', Integer, nullable=False),
          Column('patch_index', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], [
              'ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
          ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], [
              'Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
          )

SensorTractJoinTable = \
    Table('SensorTractJoin', metadata,
          Column('visit_number', Integer, nullable=False),
          Column('physical_sensor_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('tract_number', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          ForeignKeyConstraint(['visit_number', 'physical_sensor_number', 'camera_name'], [
              'ObservedSensor.visit_number', 'ObservedSensor.physical_sensor_number', 'ObservedSensor.camera_name']),
          ForeignKeyConstraint(['tract_number', 'skymap_name'], [
              'Tract.tract_number', 'Tract.skymap_name'])
          )

VisitPatchJoinTable = \
    Table('VisitPatchJoin', metadata,
          Column('visit_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('tract_number', Integer, nullable=False),
          Column('patch_index', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          ForeignKeyConstraint(['visit_number', 'camera_name'], [
              'Visit.visit_number', 'Visit.camera_name']),
          ForeignKeyConstraint(['tract_number', 'patch_index', 'skymap_name'], [
              'Patch.tract_number', 'Patch.patch_index', 'Patch.skymap_name'])
          )

VisitTractJoinTable = \
    Table('VisitTractJoin', metadata,
          Column('visit_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('tract_number', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          ForeignKeyConstraint(['visit_number', 'camera_name'], [
              'Visit.visit_number', 'Visit.camera_name']),
          ForeignKeyConstraint(['tract_number', 'skymap_name'], [
              'Tract.tract_number', 'Tract.skymap_name'])
          )

PhysicalFilterDatasetJoinTable = \
    Table('PhysicalFilterDatasetJoin', metadata,
          Column('physical_filter_name', String, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['physical_filter_name', 'camera_name'], [
              'PhysicalFilter.physical_filter_name', 'PhysicalFilter.camera_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

PhysicalSensorDatasetJoinTable = \
    Table('PhysicalSensorDatasetJoin', metadata,
          Column('physical_sensor_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['physical_sensor_number', 'camera_name'], [
              'PhysicalSensor.physical_sensor_number', 'PhysicalSensor.camera_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

VisitDatasetJoinTable = \
    Table('VisitDatasetJoin', metadata,
          Column('visit_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['visit_number', 'camera_name'], [
              'Visit.visit_number', 'Visit.camera_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

SnapDatasetJoinTable = \
    Table('SnapDatasetJoin', metadata,
          Column('snap_index', Integer, nullable=False),
          Column('visit_number', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['snap_index', 'visit_number', 'camera_name'], [
              'Snap.snap_index', 'Snap.visit_number', 'Snap.camera_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

VisitRangeDatasetJoinTable = \
    Table('VisitRangeDatasetJoin', metadata,
          Column('visit_begin', Integer, nullable=False),
          Column('visit_end', Integer, nullable=False),
          Column('camera_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['visit_begin', 'visit_end', 'camera_name'], [
              'VisitRange.visit_begin', 'VisitRange.visit_end', 'VisitRange.camera_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

AbstractFilterDatasetJoinTable = \
    Table('AbstractFilterDatasetJoin', metadata,
          Column('abstract_filter_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['abstract_filter_name'], [
              'AbstractFilter.abstract_filter_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

TractDatasetJoinTable = \
    Table('TractDatasetJoin', metadata,
          Column('tract_number', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['tract_number', 'skymap_name'], [
              'Tract.tract_number', 'Tract.skymap_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )

PatchDatasetJoinTable = \
    Table('PatchDatasetJoin', metadata,
          Column('patch_index', Integer, nullable=False),
          Column('tract_number', Integer, nullable=False),
          Column('skymap_name', String, nullable=False),
          Column('dataset_id', Integer, nullable=False, primary_key=True),
          Column('registry_id', Integer, nullable=False, primary_key=True),
          ForeignKeyConstraint(['patch_index', 'tract_number', 'skymap_name'], [
              'Patch.patch_index', 'Patch.tract_number', 'Patch.skymap_name']),
          ForeignKeyConstraint(['dataset_id', 'registry_id'], [
              'Dataset.dataset_id', 'Dataset.registry_id'])
          )
