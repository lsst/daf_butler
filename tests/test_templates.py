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

"""Test file name templating."""

import os.path
import unittest

from lsst.daf.butler import DatasetType, DatasetRef, FileTemplates, DimensionUniverse, \
    FileTemplate, FileTemplatesConfig, StorageClass, Run, FileTemplateValidationError, \
    DimensionGraph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestFileTemplates(unittest.TestCase):
    """Test creation of paths from templates."""

    def makeDatasetRef(self, datasetTypeName, dataId=None, storageClassName="DefaultStorageClass",
                       conform=True):
        """Make a simple DatasetRef"""
        if dataId is None:
            dataId = self.dataId
        datasetType = DatasetType(datasetTypeName, DimensionGraph(self.universe, names=dataId.keys()),
                                  StorageClass(storageClassName))
        return DatasetRef(datasetType, dataId, id=1, run=Run(id=2, collection="run2"), conform=conform)

    def setUp(self):
        self.universe = DimensionUniverse()
        self.dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "U"}

    def assertTemplate(self, template, answer, ref):
        fileTmpl = FileTemplate(template)
        path = fileTmpl.format(ref)
        self.assertEqual(path, answer)

    def testBasic(self):
        tmplstr = "{run:02d}/{datasetType}/{visit:05d}/{physical_filter}"
        self.assertTemplate(tmplstr,
                            "02/calexp/00052/U",
                            self.makeDatasetRef("calexp", conform=False))
        tmplstr = "{collection}/{datasetType}/{visit:05d}/{physical_filter}-trail"
        self.assertTemplate(tmplstr,
                            "run2/calexp/00052/U-trail",
                            self.makeDatasetRef("calexp", conform=False))

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("no fields at all")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{visit}")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{run}_{datasetType}")

    def testRunOrCollectionNeeded(self):
        tmplstr = "{datasetType}/{visit:05d}/{physical_filter}"
        with self.assertRaises(FileTemplateValidationError):
            self.assertTemplate(tmplstr,
                                "02/calexp/00052/U",
                                self.makeDatasetRef("calexp"))

    def testOptional(self):
        """Optional units in templates."""
        ref = self.makeDatasetRef("calexp", conform=False)
        tmplstr = "{run:02d}/{datasetType}/v{visit:05d}_f{physical_filter:?}"
        self.assertTemplate(tmplstr, "02/calexp/v00052_fU",
                            self.makeDatasetRef("calexp", conform=False))

        du = {"visit": 48, "tract": 265, "skymap": "big", "instrument": "dummy"}
        self.assertTemplate(tmplstr, "02/calexpT/v00048",
                            self.makeDatasetRef("calexpT", du, conform=False))

        # Ensure that this returns a relative path even if the first field
        # is optional
        tmplstr = "{run:02d}/{tract:?}/{visit:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "02/52/fU", ref)

        # Ensure that // from optionals are converted to singles
        tmplstr = "{run:02d}/{datasetType}/{patch:?}/{tract:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "02/calexp/fU", ref)

        # Optionals with some text between fields
        tmplstr = "{run:02d}/{datasetType}/p{patch:?}_t{tract:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "02/calexp/p/fU", ref)
        tmplstr = "{run:02d}/{datasetType}/p{patch:?}_t{visit:04d?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "02/calexp/p_t0052/fU", ref)

    def testComponent(self):
        """Test handling of components in templates."""
        refMetricOutput = self.makeDatasetRef("metric.output")
        refMetric = self.makeDatasetRef("metric")
        refMaskedImage = self.makeDatasetRef("calexp.maskedimage.variance")
        refWcs = self.makeDatasetRef("calexp.wcs")

        tmplstr = "{run:02d}_c_{component}_v{visit}"
        self.assertTemplate(tmplstr, "02_c_output_v52", refMetricOutput)

        tmplstr = "{run:02d}_{component:?}_{visit}"
        self.assertTemplate(tmplstr, "02_52", refMetric)
        self.assertTemplate(tmplstr, "02_output_52", refMetricOutput)
        self.assertTemplate(tmplstr, "02_maskedimage.variance_52", refMaskedImage)
        self.assertTemplate(tmplstr, "02_output_52", refMetricOutput)

        # Providing a component but not using it
        tmplstr = "{collection}/{datasetType}/v{visit:05d}"
        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", refWcs)

    def testFields(self):
        # Template, mandatory fields, optional non-special fields,
        # special fields, optional special fields
        testData = (("{collection}/{datasetType}/{visit:05d}/{physical_filter}-trail",
                     set(["visit", "physical_filter"]),
                     set(),
                     set(["collection", "datasetType"]),
                     set()),
                    ("{collection}/{component:?}_{visit}",
                     set(["visit"]),
                     set(),
                     set(["collection"]),
                     set(["component"]),),
                    ("{run}/{component:?}_{visit:?}_{physical_filter}_{instrument}_{datasetType}",
                     set(["physical_filter", "instrument"]),
                     set(["visit"]),
                     set(["run", "datasetType"]),
                     set(["component"]),),
                    )
        for tmplstr, mandatory, optional, special, optionalSpecial in testData:
            with self.subTest(template=tmplstr):
                tmpl = FileTemplate(tmplstr)
                fields = tmpl.fields()
                self.assertEqual(fields, mandatory)
                fields = tmpl.fields(optionals=True)
                self.assertEqual(fields, mandatory | optional)
                fields = tmpl.fields(specials=True)
                self.assertEqual(fields, mandatory | special)
                fields = tmpl.fields(specials=True, optionals=True)
                self.assertEqual(fields, mandatory | special | optional | optionalSpecial)

    def testSimpleConfig(self):
        """Test reading from config file"""
        configRoot = os.path.join(TESTDIR, "config", "templates")
        config1 = FileTemplatesConfig(os.path.join(configRoot, "templates-nodefault.yaml"))
        templates = FileTemplates(config1, universe=self.universe)
        ref = self.makeDatasetRef("calexp")
        tmpl = templates.getTemplate(ref)
        self.assertIsInstance(tmpl, FileTemplate)

        # This config file should not allow defaulting
        ref2 = self.makeDatasetRef("unknown")
        with self.assertRaises(KeyError):
            templates.getTemplate(ref2)

        # This should fall through the datasetTypeName check and use
        # StorageClass instead
        ref3 = self.makeDatasetRef("unknown2", storageClassName="StorageClassX")
        tmplSc = templates.getTemplate(ref3)
        self.assertIsInstance(tmplSc, FileTemplate)

        # Try with a component: one with defined formatter and one without
        refWcs = self.makeDatasetRef("calexp.wcs")
        refImage = self.makeDatasetRef("calexp.image")
        tmplCalexp = templates.getTemplate(ref)
        tmplWcs = templates.getTemplate(refWcs)  # Should be special
        tmpl_image = templates.getTemplate(refImage)
        self.assertIsInstance(tmplCalexp, FileTemplate)
        self.assertIsInstance(tmpl_image, FileTemplate)
        self.assertIsInstance(tmplWcs, FileTemplate)
        self.assertEqual(tmplCalexp, tmpl_image)
        self.assertNotEqual(tmplCalexp, tmplWcs)

        # Check dimensions lookup order.
        # The order should be: dataset type name, dimension, storage class
        # This one will not match name but might match storage class.
        # It should match dimensions
        refDims = self.makeDatasetRef("nomatch", dataId={"instrument": "LSST", "physical_filter": "z"},
                                      storageClassName="StorageClassX")
        tmplDims = templates.getTemplate(refDims)
        self.assertIsInstance(tmplDims, FileTemplate)
        self.assertNotEqual(tmplDims, tmplSc)

        # Test that instrument overrides retrieve specialist templates
        refPvi = self.makeDatasetRef("pvi")
        refPviHsc = self.makeDatasetRef("pvi", dataId={"instrument": "HSC", "physical_filter": "z"})
        refPviLsst = self.makeDatasetRef("pvi", dataId={"instrument": "LSST", "physical_filter": "z"})

        tmplPvi = templates.getTemplate(refPvi)
        tmplPviHsc = templates.getTemplate(refPviHsc)
        tmplPviLsst = templates.getTemplate(refPviLsst)
        self.assertEqual(tmplPvi, tmplPviLsst)
        self.assertNotEqual(tmplPvi, tmplPviHsc)

        # Have instrument match and dimensions look up with no name match
        refNoPviHsc = self.makeDatasetRef("pvix", dataId={"instrument": "HSC", "physical_filter": "z"},
                                          storageClassName="StorageClassX")
        tmplNoPviHsc = templates.getTemplate(refNoPviHsc)
        self.assertNotEqual(tmplNoPviHsc, tmplDims)
        self.assertNotEqual(tmplNoPviHsc, tmplPviHsc)

        # Format config file with defaulting
        config2 = FileTemplatesConfig(os.path.join(configRoot, "templates-withdefault.yaml"))
        templates = FileTemplates(config2, universe=self.universe)
        tmpl = templates.getTemplate(ref2)
        self.assertIsInstance(tmpl, FileTemplate)

        # Format config file with bad format string
        with self.assertRaises(FileTemplateValidationError):
            FileTemplates(os.path.join(configRoot, "templates-bad.yaml"), universe=self.universe)

        # Config file with no defaulting mentioned
        config3 = os.path.join(configRoot, "templates-nodefault2.yaml")
        templates = FileTemplates(config3, universe=self.universe)
        with self.assertRaises(KeyError):
            templates.getTemplate(ref2)

        # Try again but specify a default in the constructor
        default = "{run}/{datasetType}/{physical_filter}"
        templates = FileTemplates(config3, default=default, universe=self.universe)
        tmpl = templates.getTemplate(ref2)
        self.assertEqual(tmpl.template, default)

    def testValidation(self):
        configRoot = os.path.join(TESTDIR, "config", "templates")
        config1 = FileTemplatesConfig(os.path.join(configRoot, "templates-nodefault.yaml"))
        templates = FileTemplates(config1, universe=self.universe)

        entities = {}
        entities["calexp"] = self.makeDatasetRef("calexp", storageClassName="StorageClassX",
                                                 dataId={"instrument": "dummy", "physical_filter": "i",
                                                         "visit": 52})

        with self.assertLogs(level="WARNING") as cm:
            templates.validateTemplates(entities.values(), logFailures=True)
        self.assertIn("Unchecked keys", cm.output[0])
        self.assertIn("StorageClassX", cm.output[0])

        entities["pvi"] = self.makeDatasetRef("pvi", storageClassName="StorageClassX",
                                              dataId={"instrument": "dummy", "physical_filter": "i"})
        entities["StorageClassX"] = self.makeDatasetRef("storageClass",
                                                        storageClassName="StorageClassX",
                                                        dataId={"instrument": "dummy", "visit": 2})
        entities["calexp.wcs"] = self.makeDatasetRef("calexp.wcs",
                                                     storageClassName="StorageClassX",
                                                     dataId={"instrument": "dummy",
                                                             "physical_filter": "i", "visit": 23},
                                                     conform=False)

        entities["instrument+physical_filter"] = self.makeDatasetRef("filter_inst",
                                                                     storageClassName="StorageClassX",
                                                                     dataId={"physical_filter": "i",
                                                                             "instrument": "SCUBA"})
        entities["hsc+pvi"] = self.makeDatasetRef("pvi", storageClassName="StorageClassX",
                                                  dataId={"physical_filter": "i", "instrument": "HSC"})

        entities["hsc+instrument+physical_filter"] = self.makeDatasetRef("filter_inst",
                                                                         storageClassName="StorageClassX",
                                                                         dataId={"physical_filter": "i",
                                                                                 "instrument": "HSC"})

        templates.validateTemplates(entities.values(), logFailures=True)

        # Rerun but with a failure
        entities["pvi"] = self.makeDatasetRef("pvi", storageClassName="StorageClassX",
                                              dataId={"abstract_filter": "i"})
        with self.assertRaises(FileTemplateValidationError):
            with self.assertLogs(level="FATAL"):
                templates.validateTemplates(entities.values(), logFailures=True)


if __name__ == "__main__":
    unittest.main()
