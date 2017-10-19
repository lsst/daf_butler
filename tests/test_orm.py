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

from lsst.butler.orm.schema import Base, Dataset, Run, DatasetType

class SchemaTestCase(unittest.TestCase):
    """A test case for the ORM schema."""

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.sessionmaker = sessionmaker()
        self.sessionmaker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    def testAddDataset(self):
        session = self.sessionmaker()
        
        dataset_id = 0
        registry_id = 0
        dataset_type_name = "calexp"
        unit_pack = bytes()
        uri="http:://www.lsst.org/testdata/dataset-0"
        run_id = 0
        producer_id = None

        collection_tag = "first"
        run = Run(run_id=run_id, registry_id=registry_id, tag=collection_tag)
        session.add(run)

        storage_class = "Exposure"
        datasetType = DatasetType(name=dataset_type_name, storage_class=storage_class)
        session.add(datasetType)

        dataset = Dataset(dataset_id=dataset_id,
                          registry_id=registry_id,
                          dataset_type_name=dataset_type_name,
                          unit_pack=unit_pack,
                          uri=uri,
                          run_id=run_id,
                          producer_id=producer_id)
        session.add(dataset)

        session.commit()

        runOut = session.query(Run).filter_by(run_id=run_id).first()
        self.assertEquals(runOut.run_id, run_id)
        self.assertEquals(runOut.tag, collection_tag)

        datasetTypeOut = session.query(DatasetType).filter_by(name=dataset_type_name).first()
        self.assertEquals(datasetTypeOut.name, dataset_type_name)
        self.assertEquals(datasetTypeOut.storage_class, storage_class)

        datasetOut = session.query(Dataset).filter_by(dataset_id=dataset_id).first()
        self.assertEquals(datasetOut.dataset_id, dataset_id)
        self.assertEquals(datasetOut.registry_id, registry_id)
        self.assertEquals(datasetOut.dataset_type_name, dataset_type_name)
        self.assertEquals(datasetOut.unit_pack, unit_pack)
        self.assertEquals(datasetOut.uri, uri)
        self.assertEquals(datasetOut.run_id, run_id)
        self.assertEquals(datasetOut.producer_id, producer_id)
        