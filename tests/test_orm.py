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
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import unittest
from sqlalchemy import create_engine, LargeBinary
from sqlalchemy.orm import sessionmaker

from lsst.butler.orm.schema import Base, Dataset, Run, DatasetType, Quantum

class SchemaTestCase(unittest.TestCase):
    """A test case for the ORM schema."""

    def setUp(self):
        self.createDatabase()
        self.populateTables()

    def createDatabase(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.sessionmaker = sessionmaker()
        self.sessionmaker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    def populateTables(self):
        self.registry_id = 0
        self.run_id = 0
        self.collection_tag = "default"
        self.dataset_type_name = "calexp"
        self.storage_class = "Exposure"
        self.dataset_id = 0
        self.unit_pack = bytes()
        self.uri="http:://www.lsst.org/testdata/dataset-0"
        self.producer_id = None
        self.quantum_id = 0
        self.task = "TestTask"

        session = self.sessionmaker()

        # Run
        run = Run(run_id=self.run_id, registry_id=self.registry_id, tag=self.collection_tag)
        session.add(run)

        # DatasetType
        datasetType = DatasetType(name=self.dataset_type_name, storage_class=self.storage_class)
        session.add(datasetType)

        # Dataset
        dataset = Dataset(dataset_id=self.dataset_id,
                          registry_id=self.registry_id,
                          dataset_type_name=self.dataset_type_name,
                          unit_pack=self.unit_pack,
                          uri=self.uri,
                          run_id=self.run_id,
                          producer_id=self.producer_id)
        session.add(dataset)

        # Quantum
        quantum = Quantum(quantum_id=self.quantum_id,
                          registry_id=self.registry_id,
                          run_id=self.run_id,
                          task=self.task)
        session.add(quantum)
        
        session.commit()

    def testRun(self):
        session = self.sessionmaker()
        out = session.query(Run).filter_by(run_id=self.run_id).first()
        self.assertEquals(out.run_id, self.run_id)
        self.assertEquals(out.tag, self.collection_tag)

    def testDatasetType(self):
        session = self.sessionmaker()
        out = session.query(DatasetType).filter_by(name=self.dataset_type_name).first()
        self.assertEquals(out.name, self.dataset_type_name)
        self.assertEquals(out.storage_class, self.storage_class)

    def testDataset(self):
        session = self.sessionmaker()
        out = session.query(Dataset).filter_by(dataset_id=self.dataset_id).first()
        self.assertEquals(out.dataset_id, self.dataset_id)
        self.assertEquals(out.registry_id, self.registry_id)
        self.assertEquals(out.dataset_type_name, self.dataset_type_name)
        self.assertEquals(out.unit_pack, self.unit_pack)
        self.assertEquals(out.uri, self.uri)
        self.assertEquals(out.run_id, self.run_id)
        self.assertEquals(out.producer_id, self.producer_id)
        
    def testQuantum(self):
        session = self.sessionmaker()
        quantumOut = session.query(Quantum).filter_by(quantum_id=self.quantum_id).first()
        self.assertEquals(quantumOut.quantum_id, self.quantum_id)
        self.assertEquals(quantumOut.registry_id, self.registry_id)
        self.assertEquals(quantumOut.run_id, self.run_id)
        self.assertEquals(quantumOut.task, self.task)

