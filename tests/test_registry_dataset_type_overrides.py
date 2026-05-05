# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from lsst.daf.butler import Butler, Config, DatasetRef, DatasetType, MissingDatasetTypeError
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel
from lsst.daf.butler.tests.utils import safeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class RegistryDatasetTypeOverridesTestCase(unittest.TestCase):
    """Tests for overriding dataset type names and storage classes in registry
    configuration.
    """

    def setUp(self) -> None:
        model_type_name = "lsst.daf.butler.tests.dict_convertible_model.DictConvertibleModel"
        config = Config()
        # Configure two storage classes that are bidirectional convertible
        # but with different Python types, and a third that is not
        # convertible to either.
        config["storageClasses"] = {
            "BuiltinDict": {
                "pytype": "dict",
                "converters": {model_type_name: f"{model_type_name}.to_dict"},
            },
            "DictModel": {
                "pytype": model_type_name,
                "converters": {"dict": f"{model_type_name}.from_dict"},
            },
            "BuiltinList": {"pytype": "list"},
        }
        config["datastore"] = {
            "formatters": {
                "BuiltinDict": "lsst.daf.butler.formatters.yaml.YamlFormatter",
                "DictModel": "lsst.daf.butler.formatters.json.JsonFormatter",
                "BuiltinList": "lsst.daf.butler.formatters.json.JsonFormatter",
            }
        }
        repo_dir = self.enterContext(safeTestTempDir(TESTDIR))
        self.base_config = Butler.makeRepo(root=repo_dir, config=config)
        self.base_butler = self.enterContext(Butler.from_config(self.base_config, writeable=True))

    def test_rename(self) -> None:
        """Test renaming two dataset types in config overrides, to mimic the
        case where and old dataset type is squatting on a name we want to use
        for a new one.
        """
        self.setup_rename()
        self.base_butler.registry.registerDatasetType(self.legacy_dst)
        self.base_butler.registry.registerDatasetType(self.future_dst)
        with Butler.from_config(self.override_config) as override_butler:
            # Butler with overrides sees the rename:
            self.assertEqual(override_butler.get_dataset_type("legacy_dst"), self.legacy_dst_override)
            self.assertEqual(override_butler.get_dataset_type("dst"), self.future_dst_override)
            with self.assertRaises(MissingDatasetTypeError):
                override_butler.get_dataset_type("future_dst")
            # Original butler still sees the original definitions:
            self.assertEqual(self.base_butler.get_dataset_type("dst"), self.legacy_dst)
            self.assertEqual(self.base_butler.get_dataset_type("future_dst"), self.future_dst)
            with self.assertRaises(MissingDatasetTypeError):
                self.base_butler.get_dataset_type("legacy_dst")
        # Test dataset type queries with a new butler to hit different
        # caching/fetching paths.
        with Butler.from_config(self.override_config) as override_butler:
            self.assertCountEqual(
                override_butler.registry.queryDatasetTypes("*"),
                [self.legacy_dst_override, self.future_dst_override],
            )
        # Test put/get and dataset queries across the two butlers.
        with Butler.from_config(self.override_config, writeable=True) as override_butler:
            legacy_ref = self.base_butler.put({"one": 1}, "dst", run="run1")
            future_ref_override = override_butler.put([2], "dst", instrument="Cam1", run="run1")
            self.base_butler.registry.refresh()
            override_butler.registry.refresh()
            self.assertEqual(self.base_butler.get("future_dst", instrument="Cam1", collections=["run1"]), [2])
            self.assertEqual(override_butler.get("legacy_dst", collections=["run1"]), {"one": 1})
            self.assertEqual(
                self.base_butler.getURI("future_dst", instrument="Cam1", collections=["run1"]).getExtension(),
                ".json",
            )
            self.assertEqual(
                override_butler.getURI("legacy_dst", collections=["run1"]).getExtension(), ".yaml"
            )
            self.assertEqual(
                self.base_butler.query_datasets("future_dst", collections=["run1"]),
                [DatasetRef(self.future_dst, future_ref_override.dataId, "run1", id=future_ref_override.id)],
            )
            self.assertEqual(
                override_butler.query_datasets("legacy_dst", collections=["run1"]),
                [DatasetRef(self.legacy_dst_override, legacy_ref.dataId, "run1", id=legacy_ref.id)],
            )

    def test_rename_registration(self) -> None:
        """Test dataset type registration when the dataset type name has been
        configured to be renamed.
        """
        self.setup_rename()
        with Butler.from_config(self.override_config, writeable=True) as override_butler:
            with self.assertRaises(ConflictingDefinitionError):
                # Even though the original name of a renamed dataset type
                # appears missing, we shouldn't be able to register it (this
                # is the only way in which an override rename should differ
                # from a real DB-level rename).
                override_butler.registry.registerDatasetType(self.future_dst)
            # We can register the original dataset type in the base repo by
            # registering its rename in the override repo.
            override_butler.registry.registerDatasetType(self.future_dst_override)
            self.base_butler.registry.refresh()
            self.assertEqual(self.base_butler.get_dataset_type("future_dst"), self.future_dst)

    def setup_rename(self) -> None:
        """Do additional test setup for rename-override tests."""
        self.legacy_dst = DatasetType(
            "dst", dimensions=(), storageClass="BuiltinDict", universe=self.base_butler.dimensions
        )
        self.future_dst = DatasetType(
            "future_dst",
            dimensions={"instrument"},
            storageClass="BuiltinList",
            universe=self.base_butler.dimensions,
        )
        self.base_butler.registry.insertDimensionData("instrument", {"name": "Cam1"})
        self.base_butler.collections.register("run1")
        self.override_config = self.base_config.copy()
        self.override_config[".registry.managers.datasets"] = {
            "cls": self.base_config[".registry.managers.datasets"],
            "config": {
                "overrides": {
                    "dst": {"rename": "legacy_dst"},
                    "future_dst": {"rename": "dst"},
                }
            },
        }
        self.legacy_dst_override = DatasetType(
            "legacy_dst", dimensions=(), storageClass="BuiltinDict", universe=self.base_butler.dimensions
        )
        self.future_dst_override = DatasetType(
            "dst",
            dimensions={"instrument"},
            storageClass="BuiltinList",
            universe=self.base_butler.dimensions,
        )

    def test_storage_class_override_config(self) -> None:
        """Test overriding the storage class of a dataset type via a repository
        configuration override.
        """
        self.setup_storage_class()
        self.base_butler.registry.registerDatasetType(self.base_dst)
        with Butler.from_config(self.override_config, writeable=True) as override_butler:
            self.assertEqual(self.base_butler.get_dataset_type("dst"), self.base_dst)
            self.assertEqual(override_butler.get_dataset_type("dst"), self.override_dst)
            # 'put' with both butlers using their natural storage classes.
            obj1 = {"one": "1"}
            obj2 = DictConvertibleModel(content={"two": "2"}, extra="three")
            ref1 = self.base_butler.put(obj1, "dst", run="run1")
            ref2 = override_butler.put(obj2, "dst", run="run2")
            self.base_butler.registry.refresh()
            override_butler.registry.refresh()
            # We can 'get' using the refs with either butler, since that's
            # just a call-level dataset type override that makes the datastore
            # 'get' match the storage class used for the write.
            self.assertEqual(self.base_butler.get(ref1), obj1)
            self.assertEqual(self.base_butler.get(ref2), obj2)
            self.assertEqual(override_butler.get(ref1), obj1)
            self.assertEqual(override_butler.get(ref2), obj2)
            # Check that we used the right formatters by looking at the
            # extensions.
            self.assertEqual(self.base_butler.getURI(ref1).getExtension(), ".yaml")
            self.assertEqual(self.base_butler.getURI(ref2).getExtension(), ".json")
            # Do a 'get' with just the dataset type name with the same butler
            # we used to do the write; no conversion necessary.
            self.assertEqual(self.base_butler.get("dst", collections=["run1"]), obj1)
            self.assertEqual(override_butler.get("dst", collections=["run2"]), obj2)
            # Do a 'get' with just the dataset type with the other butlers.
            # This should do a storage class conversion.
            self.assertEqual(self.base_butler.get("dst", collections=["run2"]), obj2.to_dict())
            self.assertEqual(
                override_butler.get("dst", collections=["run1"]), DictConvertibleModel.from_dict(obj1)
            )
            # Do a 'get' with call-level storage class overrides that differ
            # from the client's storage class.
            self.assertEqual(
                self.base_butler.get("dst", collections=["run1"], storageClass="DictModel"),
                DictConvertibleModel.from_dict(obj1),
            )
            self.assertEqual(
                override_butler.get("dst", collections=["run2"], storageClass="BuiltinDict"), obj2.to_dict()
            )
            # Query for datasets and check that the refs have the right
            # storage classes.
            self.assertCountEqual(
                self.base_butler.query_datasets("dst", collections=["run1", "run2"], find_first=False),
                [ref1, ref2.overrideStorageClass("BuiltinDict")],
            )
            self.assertCountEqual(
                override_butler.query_datasets("dst", collections=["run1", "run2"], find_first=False),
                [ref1.overrideStorageClass("DictModel"), ref2],
            )

    def test_storage_class_override_config_registration(self) -> None:
        """Test registering dataset types whose storage classes have been
        overridden via repository configuration.
        """
        self.setup_storage_class()
        with Butler.from_config(self.override_config, writeable=True) as override_butler:
            # Trying to register a dataset type with a storage class override
            # when it doesn't already exist is an error, because we don't know
            # what it's storage class should really be in the database.
            with self.assertRaises(ConflictingDefinitionError):
                override_butler.registry.registerDatasetType(self.override_dst)
            # Registering in the base repository is of course fine.
            self.base_butler.registry.registerDatasetType(self.base_dst)
            # Re-registering with the new storage class in the override repo
            # should be a no-op, because it should look like it already exists
            # with that storage class.
            self.assertFalse(override_butler.registry.registerDatasetType(self.override_dst))
            # Re-registering with the base repo's storage class in the
            # override repo should be an error, because to the override
            # repo it looks like it has a different storage class.
            with self.assertRaises(ConflictingDefinitionError):
                override_butler.registry.registerDatasetType(self.base_dst)

    def setup_storage_class(self) -> None:
        """Do additional test setup for storage-class-override tests."""
        self.base_dst = DatasetType(
            "dst", dimensions=(), storageClass="BuiltinDict", universe=self.base_butler.dimensions
        )
        self.override_dst = self.base_dst.overrideStorageClass("DictModel")
        self.base_butler.collections.register("run1")
        self.base_butler.collections.register("run2")
        self.override_config = self.base_config.copy()
        self.override_config[".registry.managers.datasets"] = {
            "cls": self.base_config[".registry.managers.datasets"],
            "config": {
                "overrides": {
                    "dst": {"storageClass": "DictModel"},
                }
            },
        }


if __name__ == "__main__":
    unittest.main()
