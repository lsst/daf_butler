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
        self.registry_id = 0
        self.run_id = 0
        self.collection_tag = "default"
        
        self.createDatabase()
        self.createRun(self.run_id, self.collection_tag)

    def createDatabase(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.sessionmaker = sessionmaker()
        self.sessionmaker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    def createRun(self, run_id, collection_tag):
        session = self.sessionmaker()
        run = Run(run_id=run_id, registry_id=self.registry_id, tag=collection_tag)
        session.add(run)
        session.commit()

    def testRun(self):
        session = self.sessionmaker()
        runOut = session.query(Run).filter_by(run_id=self.run_id).first()
        self.assertEquals(runOut.run_id, self.run_id)
        self.assertEquals(runOut.tag, self.collection_tag)

    def testAddDataset(self):
        dataset_id = 0
        dataset_type_name = "calexp"
        unit_pack = bytes()
        uri="http:://www.lsst.org/testdata/dataset-0"
        producer_id = None
        
        session = self.sessionmaker()
        
        storage_class = "Exposure"
        datasetType = DatasetType(name=dataset_type_name, storage_class=storage_class)
        session.add(datasetType)

        dataset = Dataset(dataset_id=dataset_id,
                          registry_id=self.registry_id,
                          dataset_type_name=dataset_type_name,
                          unit_pack=unit_pack,
                          uri=uri,
                          run_id=self.run_id,
                          producer_id=producer_id)
        session.add(dataset)

        session.commit()

        datasetTypeOut = session.query(DatasetType).filter_by(name=dataset_type_name).first()
        self.assertEquals(datasetTypeOut.name, dataset_type_name)
        self.assertEquals(datasetTypeOut.storage_class, storage_class)

        datasetOut = session.query(Dataset).filter_by(dataset_id=dataset_id).first()
        self.assertEquals(datasetOut.dataset_id, dataset_id)
        self.assertEquals(datasetOut.registry_id, self.registry_id)
        self.assertEquals(datasetOut.dataset_type_name, dataset_type_name)
        self.assertEquals(datasetOut.unit_pack, unit_pack)
        self.assertEquals(datasetOut.uri, uri)
        self.assertEquals(datasetOut.run_id, self.run_id)
        self.assertEquals(datasetOut.producer_id, producer_id)
        
    def testQuantum(self):
        quantum_id = 0
        task = "TestTask"

        session = self.sessionmaker()
        quantum = Quantum(quantum_id=quantum_id,
                            registry_id=self.registry_id,
                            run_id=self.run_id,
                            task=task)
        session.add(quantum)
        session.commit()

        quantumOut = session.query(Quantum).filter_by(quantum_id=quantum_id).first()
        self.assertEquals(quantumOut.quantum_id, quantum_id)
        self.assertEquals(quantumOut.registry_id, self.registry_id)
        self.assertEquals(quantumOut.run_id, self.run_id)
        self.assertEquals(quantumOut.task, task)

