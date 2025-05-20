import os
import tempfile
import unittest
from lsst.daf.butler import Butler, Config, DatasetType, CollectionType, Timespan
from lsst.daf.butler.script.exportCalibs import exportCalibs
from lsst.daf.butler.script.certifyCalibrations import certifyCalibrations
from lsst.daf.butler.tests import makeTestRepo


class ExportCalibsTestCase(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for the test repository
        self.root = tempfile.mkdtemp()
        self.repo = os.path.join(self.root, "repo")
        self.export_dir = os.path.join(self.root, "export")
        dataIds = {"instrument": ["TestCam"], "physical_filter": ["d-r"]}
        config = Config()
        config["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
        self.butler = makeTestRepo(self.repo, dataIds, config=config)
        self.registry = self.butler.registry
        universe = self.registry.dimensions

        # Define dataset types
        self.bias_type = DatasetType("bias", ("instrument", "physical_filter"),
                                     "StructuredDataDict", universe=universe,
                                     isCalibration=True)
        self.dark_type = DatasetType("dark", ("instrument", "physical_filter"),
                                     "StructuredDataDict", universe=universe,
                                     isCalibration=True)
        self.registry.registerDatasetType(self.bias_type)
        self.registry.registerDatasetType(self.dark_type)

        # Register RUN collections (normal data runs)
        self.bias_run = "run/bias"
        self.dark_run = "run/dark"
        self.empty_run = "run/empty"

        self.registry.registerCollection(self.bias_run, CollectionType.RUN)
        self.registry.registerCollection(self.dark_run, CollectionType.RUN)
        self.registry.registerCollection(self.empty_run, CollectionType.RUN)

        # Insert datasets into bias and dark collections
        self.data_id = {"instrument": "TestCam", "physical_filter": "d-r"}
        self.butler.put({"bias": "bias_data"}, self.bias_type, self.data_id, run=self.bias_run)
        self.butler.put({"dark": "dark_data"}, self.dark_type, self.data_id, run=self.dark_run)

        # Register calibration collections
        self.bias_calib = "calib/bias"
        self.dark_calib = "calib/dark"
        self.empty_calib = "calib/empty"
        certifyCalibrations(self.repo, self.bias_run, self.bias_calib, "bias",
                            begin_date=None, end_date=None, search_all_inputs=False)
        certifyCalibrations(self.repo, self.dark_run, self.dark_calib, "dark",
                            begin_date=None, end_date=None, search_all_inputs=False)

        # Create a calibration chain
        self.calib_chain = "calib/chain"
        chain = [self.bias_calib, self.dark_calib, self.empty_run]
        self.registry.registerCollection(self.calib_chain, CollectionType.CHAINED)
        self.registry.setCollectionChain(self.calib_chain, chain)

    def test_export_calibs_filters_collections(self):
        # Export calibrations for 'bias' dataset type
        exportCalibs(
            repo=self.repo,
            directory=self.export_dir,
            collections=[self.calib_chain],
            dataset_type=["bias"],
            transfer="direct"
        )

        # Read the exported YAML
        export_yaml = os.path.join(self.export_dir, "export.yaml")
        with open(export_yaml, "r") as f:
            content = f.read()

        # Verify that only 'calib/bias' is included
        self.assertIn(self.bias_calib, content)
        self.assertNotIn(self.dark_calib, content)
        self.assertNotIn(self.empty_run, content)

    def tearDown(self):
        # Clean up temporary directories
        import shutil
        shutil.rmtree(self.root)


if __name__ == "__main__":
    unittest.main()
