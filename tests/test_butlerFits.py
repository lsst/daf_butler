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

import os
import unittest
import tempfile
import shutil
import string
import random

try:
    import boto3
    import botocore
    from moto import mock_s3
except ImportError:
    boto3 = None

    def mock_s3(cls):
        """A no-op decorator in case moto mock_s3 can not be imported.
        """
        return cls

import lsst.utils.tests

from lsst.daf.butler import Butler, Config
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType
from lsst.daf.butler.core.location import ButlerURI
from datasetsHelper import FitsCatalogDatasetsHelper, DatasetTestHelper

try:
    import lsst.afw.image
    from lsst.afw.image import LOCAL
    from lsst.geom import Box2I, Point2I, Extent2I
except ImportError:
    lsst.afw = None

TESTDIR = os.path.dirname(__file__)


class ButlerFitsTests(FitsCatalogDatasetsHelper, DatasetTestHelper):
    useTempRoot = True

    @staticmethod
    def registerDatasetTypes(datasetTypeName, dimensions, storageClass, registry):
        """Bulk register DatasetTypes
        """
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(datasetType)

        for compName, compStorageClass in storageClass.components.items():
            compType = DatasetType(datasetType.componentTypeName(compName), dimensions, compStorageClass)
            registry.registerDatasetType(compType)

    @classmethod
    def setUpClass(cls):
        if lsst.afw is None:
            raise unittest.SkipTest("afw not available.")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self):
        """Create a new butler root for each test."""
        if self.useTempRoot:
            self.root = tempfile.mkdtemp(dir=TESTDIR)
            Butler.makeRepo(self.root, config=Config(self.configFile))
            self.tmpConfigFile = os.path.join(self.root, "butler.yaml")
        else:
            self.root = None
            self.tmpConfigFile = self.configFile

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testExposureCompositePutGetConcrete(self):
        storageClass = self.storageClassFactory.getStorageClass("ExposureF")
        self.runExposureCompositePutGetTest(storageClass, "calexp")

    def testExposureCompositePutGetVirtual(self):
        storageClass = self.storageClassFactory.getStorageClass("ExposureCompositeF")
        self.runExposureCompositePutGetTest(storageClass, "unknown")

    def runExposureCompositePutGetTest(self, storageClass, datasetTypeName):
        example = os.path.join(TESTDIR, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        butler = Butler(self.tmpConfigFile)
        dimensions = butler.registry.dimensions.extract(["instrument", "visit"])
        self.registerDatasetTypes(datasetTypeName, dimensions, storageClass, butler.registry)
        dataId = {"visit": 42, "instrument": "DummyCam", "physical_filter": "d-r"}
        # Add needed Dimensions
        butler.registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
        butler.registry.addDimensionEntry("physical_filter", {"instrument": "DummyCam",
                                          "physical_filter": "d-r"})
        butler.registry.addDimensionEntry("visit", {"instrument": "DummyCam", "visit": 42,
                                                    "physical_filter": "d-r"})
        butler.put(exposure, datasetTypeName, dataId)
        # Get the full thing
        butler.get(datasetTypeName, dataId)
        # TODO enable check for equality (fix for Exposure type)
        # self.assertEqual(full, exposure)
        # Get a component
        compsRead = {}
        for compName in ("wcs", "image", "mask", "coaddInputs", "psf"):
            compTypeName = DatasetType.nameWithComponent(datasetTypeName, compName)
            component = butler.get(compTypeName, dataId)
            # TODO enable check for component instance types
            # compRef = butler.registry.find(butler.run.collection,
            #                                f"calexp.{compName}", dataId)
            # self.assertIsInstance(component,
            #                       compRef.datasetType.storageClass.pytype)
            compsRead[compName] = component
        # Simple check of WCS
        bbox = Box2I(Point2I(0, 0), Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(compsRead["wcs"], exposure.getWcs(), bbox)

        # With parameters
        inBBox = Box2I(minimum=Point2I(0, 0), maximum=Point2I(3, 3))
        parameters = dict(bbox=inBBox, origin=LOCAL)
        subset = butler.get(datasetTypeName, dataId, parameters=parameters)
        outBBox = subset.getBBox()
        self.assertEqual(inBBox, outBBox)


class PosixDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")


class InMemoryDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """InMemoryDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    useTempRoot = False


class ChainedDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3DatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """S3Datastore specialization of a butler; an S3 storage Datastore +
    a local in-memory SqlRegistry.
    """
    configFile = os.path.join(TESTDIR, "config/basic/butler-s3store.yaml")

    bucketName = "anybucketname"
    """Name of the Bucket that will be used in the tests. The name is read from
    the config file used with the tests during set-up.
    """

    root = "butlerRoot/"
    """Root repository directory expected to be used in case useTempRoot=False.
    Otherwise the root is set to a 20 characters long randomly generated string
    during set-up.
    """

    def genRoot(self):
        """Returns a random string of len 20 to serve as a root
        name for the temporary bucket repo.

        This is equivalent to tempfile.mkdtemp as this is what self.root
        becomes when useTempRoot is True.
        """
        rndstr = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
        )
        return rndstr + "/"

    def setUp(self):
        config = Config(self.configFile)
        uri = ButlerURI(config[".datastore.datastore.root"])
        self.bucketName = uri.netloc

        if self.useTempRoot:
            self.root = self.genRoot()
        rooturi = f"s3://{self.bucketName}/{self.root}"
        config.update({"datastore": {"datastore": {"root": rooturi}}})

        # set up some fake credentials if they do not exist
        if not os.path.exists("~/.aws/credentials"):
            if "AWS_ACCESS_KEY_ID" not in os.environ:
                os.environ["AWS_ACCESS_KEY_ID"] = "dummyAccessKeyId"
            if "AWS_SECRET_ACCESS_KEY" not in os.environ:
                os.environ["AWS_SECRET_ACCESS_KEY"] = "dummySecreyAccessKey"

        # MOTO needs to know that we expect Bucket bucketname to exist
        # (this used to be the class attribute bucketName)
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucketName)

        self.datastoreStr = f"datastore={self.root}"
        self.datastoreName = [f"S3Datastore@{rooturi}"]
        Butler.makeRepo(rooturi, config=config, forceConfigRoot=False)
        self.tmpConfigFile = os.path.join(rooturi, "butler.yaml")

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as err:
            errorcode = err.response["ResponseMetadata"]["HTTPStatusCode"]
            if errorcode == 404:
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # unset any potentially set dummy credentials
        keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        for key in keys:
            if key in os.environ and "dummy" in os.environ[key]:
                del os.environ[key]


if __name__ == "__main__":
    unittest.main()
