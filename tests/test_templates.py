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

"""Test file name templating."""

import os.path
import unittest
import uuid

from lsst.daf.butler import (
    DataCoordinate,
    DatasetId,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    StorageClass,
)
from lsst.daf.butler.datastore.file_templates import (
    FileTemplate,
    FileTemplates,
    FileTemplatesConfig,
    FileTemplateValidationError,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))

PlaceHolder = StorageClass("PlaceHolder")

REFUUID = DatasetId(int=uuid.uuid4().int)


class TestFileTemplates(unittest.TestCase):
    """Test creation of paths from templates."""

    def makeDatasetRef(
        self, datasetTypeName, dataId=None, storageClassName="DefaultStorageClass", run="run2", conform=True
    ):
        """Make a simple DatasetRef"""
        if dataId is None:
            dataId = self.dataId
        if "physical_filter" in dataId and "band" not in dataId:
            dataId["band"] = "b"  # Add fake band.
        dimensions = self.universe.conform(dataId.keys())
        dataId = DataCoordinate.standardize(dataId, dimensions=dimensions)

        # Pretend we have a parent if this looks like a composite
        compositeName, componentName = DatasetType.splitDatasetTypeName(datasetTypeName)
        parentStorageClass = PlaceHolder if componentName else None

        datasetType = DatasetType(
            datasetTypeName,
            dimensions,
            StorageClass(storageClassName),
            parentStorageClass=parentStorageClass,
        )
        return DatasetRef(datasetType, dataId, id=REFUUID, run=run, conform=conform)

    def setUp(self):
        self.universe = DimensionUniverse()
        self.dataId = {
            "instrument": "dummy",
            "visit": 52,
            "physical_filter": "Most Amazing U Filter Ever",
            "day_obs": 20200101,
        }

    def assertTemplate(self, template, answer, ref):
        fileTmpl = FileTemplate(template)
        path = fileTmpl.format(ref)
        self.assertEqual(path, answer)

    def testBasic(self):
        tmplstr = "{run}/{datasetType}/{visit:05d}/{physical_filter}"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/00052/Most_Amazing_U_Filter_Ever",
            self.makeDatasetRef("calexp"),
        )
        tmplstr = "{run}/{datasetType}/{visit:05d}/{physical_filter}-trail"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/00052/Most_Amazing_U_Filter_Ever-trail",
            self.makeDatasetRef("calexp"),
        )

        tmplstr = "{run}/{datasetType}/{visit:05d}/{physical_filter}-trail-{run}"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/00052/Most_Amazing_U_Filter_Ever-trail-run2",
            self.makeDatasetRef("calexp"),
        )
        self.assertTemplate(
            tmplstr,
            "run_2/calexp/00052/Most_Amazing_U_Filter_Ever-trail-run_2",
            self.makeDatasetRef("calexp", run="run/2"),
        )

        # Check that the id is sufficient without any other information.
        self.assertTemplate("{id}", str(REFUUID), self.makeDatasetRef("calexp", run="run2"))

        self.assertTemplate("{run}/{id}", f"run2/{str(REFUUID)}", self.makeDatasetRef("calexp", run="run2"))

        self.assertTemplate(
            "fixed/{id}",
            f"fixed/{str(REFUUID)}",
            self.makeDatasetRef("calexp", run="run2"),
        )

        self.assertTemplate(
            "fixed/{id}_{physical_filter}",
            f"fixed/{str(REFUUID)}_Most_Amazing_U_Filter_Ever",
            self.makeDatasetRef("calexp", run="run2"),
        )

        # Retain any "/" in run
        tmplstr = "{run:/}/{datasetType}/{visit:05d}/{physical_filter}-trail-{run}"
        self.assertTemplate(
            tmplstr,
            "run/2/calexp/00052/Most_Amazing_U_Filter_Ever-trail-run_2",
            self.makeDatasetRef("calexp", run="run/2"),
        )

        # Check that "." are replaced in the file basename, but not directory.
        dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "g.10", "day_obs": 20250101}
        self.assertTemplate(
            tmplstr,
            "run.2/calexp/00052/g_10-trail-run_2",
            self.makeDatasetRef("calexp", run="run.2", dataId=dataId),
        )

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("no fields at all")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{visit}")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{run}_{datasetType}")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{id}/fixed")

        with self.assertRaises(FileTemplateValidationError):
            FileTemplate("{run}/../{datasetType}_{visit}")

    def testAlternates(self):
        tmplstr = "{run}/{datasetType}/{visit:05d}/{physical_filter|day_obs}_{day_obs|physical_filter}"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/00052/Most_Amazing_U_Filter_Ever_20200101",
            self.makeDatasetRef("calexp"),
        )
        tmplstr = "{run}/{datasetType}/{exposure|visit:05d}/{physical_filter|day_obs}_{group|exposure:?}"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/00052/Most_Amazing_U_Filter_Ever",
            self.makeDatasetRef("calexp"),
        )

    def testRunOrCollectionNeeded(self):
        tmplstr = "{datasetType}/{visit:05d}/{physical_filter}"
        with self.assertRaises(FileTemplateValidationError):
            self.assertTemplate(tmplstr, "run2/calexp/00052/U", self.makeDatasetRef("calexp"))

    def testNoRecord(self):
        # Attaching records is not possible in this test code but we can check
        # that a missing record when a metadata entry has been requested
        # does fail.
        tmplstr = "{run}/{datasetType}/{visit.name}/{physical_filter}"
        with self.assertRaises(RuntimeError) as cm:
            self.assertTemplate(tmplstr, "", self.makeDatasetRef("calexp"))
        self.assertIn("No metadata", str(cm.exception))

    def testOptional(self):
        """Optional units in templates."""
        ref = self.makeDatasetRef("calexp")
        tmplstr = "{run}/{datasetType}/v{visit:05d}_f{physical_filter:?}_{skypix:?}"
        self.assertTemplate(
            tmplstr,
            "run2/calexp/v00052_fMost_Amazing_U_Filter_Ever",
            self.makeDatasetRef("calexp"),
        )

        du = {"visit": 48, "tract": 265, "skymap": "big", "instrument": "dummy", "htm7": 12345}
        self.assertTemplate(tmplstr, "run2/calexpT/v00048_12345", self.makeDatasetRef("calexpT", du))

        # Ensure that this returns a relative path even if the first field
        # is optional
        tmplstr = "{run}/{tract:?}/{visit:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "run2/52/fMost_Amazing_U_Filter_Ever", ref)

        # Ensure that // from optionals are converted to singles
        tmplstr = "{run}/{datasetType}/{patch:?}/{tract:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "run2/calexp/fMost_Amazing_U_Filter_Ever", ref)

        # Optionals with some text between fields
        tmplstr = "{run}/{datasetType}/p{patch:?}_t{tract:?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "run2/calexp/p/fMost_Amazing_U_Filter_Ever", ref)
        tmplstr = "{run}/{datasetType}/p{patch:?}_t{visit:04d?}/f{physical_filter}"
        self.assertTemplate(tmplstr, "run2/calexp/p_t0052/fMost_Amazing_U_Filter_Ever", ref)

    def testComponent(self):
        """Test handling of components in templates."""
        refMetricOutput = self.makeDatasetRef("metric.output")
        refMetric = self.makeDatasetRef("metric")
        refMaskedImage = self.makeDatasetRef("calexp.maskedimage.variance")
        refWcs = self.makeDatasetRef("calexp.wcs")

        tmplstr = "{run}_c_{component}_v{visit}"
        self.assertTemplate(tmplstr, "run2_c_output_v52", refMetricOutput)

        # We want this template to have both a directory and basename, to
        # test that the right parts of the output are replaced.
        tmplstr = "{component:?}/{run}_{component:?}_{visit}"
        self.assertTemplate(tmplstr, "run2_52", refMetric)
        self.assertTemplate(tmplstr, "output/run2_output_52", refMetricOutput)
        self.assertTemplate(tmplstr, "maskedimage.variance/run2_maskedimage_variance_52", refMaskedImage)
        self.assertTemplate(tmplstr, "output/run2_output_52", refMetricOutput)

        # Providing a component but not using it
        tmplstr = "{run}/{datasetType}/v{visit:05d}"
        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", refWcs)

    def testFields(self):
        # Template, mandatory fields, optional non-special fields,
        # special fields, optional special fields
        testData = (
            (
                "{run}/{datasetType}/{visit:05d}/{physical_filter}-trail",
                {"visit", "physical_filter"},
                set(),
                {"run", "datasetType"},
                set(),
            ),
            (
                "{run}/{component:?}_{visit}",
                {"visit"},
                set(),
                {"run"},
                {"component"},
            ),
            (
                "{run}/{component:?}_{visit:?}_{physical_filter}_{instrument}_{datasetType}",
                {"physical_filter", "instrument"},
                {"visit"},
                {"run", "datasetType"},
                {"component"},
            ),
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
        refDims = self.makeDatasetRef(
            "nomatch", dataId={"instrument": "LSST", "physical_filter": "z"}, storageClassName="StorageClassX"
        )
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
        refNoPviHsc = self.makeDatasetRef(
            "pvix", dataId={"instrument": "HSC", "physical_filter": "z"}, storageClassName="StorageClassX"
        )
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
        entities["calexp"] = self.makeDatasetRef(
            "calexp",
            storageClassName="StorageClassX",
            dataId={"instrument": "dummy", "physical_filter": "i", "visit": 52},
        )

        with self.assertLogs(level="WARNING") as cm:
            templates.validateTemplates(entities.values(), logFailures=True)
        self.assertIn("Unchecked keys", cm.output[0])
        self.assertIn("StorageClassX", cm.output[0])

        entities["pvi"] = self.makeDatasetRef(
            "pvi", storageClassName="StorageClassX", dataId={"instrument": "dummy", "physical_filter": "i"}
        )
        entities["StorageClassX"] = self.makeDatasetRef(
            "storageClass", storageClassName="StorageClassX", dataId={"instrument": "dummy", "visit": 2}
        )
        entities["calexp.wcs"] = self.makeDatasetRef(
            "calexp.wcs",
            storageClassName="StorageClassX",
            dataId={"instrument": "dummy", "physical_filter": "i", "visit": 23},
            conform=False,
        )

        entities["instrument+physical_filter"] = self.makeDatasetRef(
            "filter_inst",
            storageClassName="StorageClassX",
            dataId={"physical_filter": "i", "instrument": "SCUBA"},
        )
        entities["hsc+pvi"] = self.makeDatasetRef(
            "pvi", storageClassName="StorageClassX", dataId={"physical_filter": "i", "instrument": "HSC"}
        )

        entities["hsc+instrument+physical_filter"] = self.makeDatasetRef(
            "filter_inst",
            storageClassName="StorageClassX",
            dataId={"physical_filter": "i", "instrument": "HSC"},
        )

        entities["metric6"] = self.makeDatasetRef(
            "filter_inst",
            storageClassName="Integer",
            dataId={"physical_filter": "i", "instrument": "HSC"},
        )

        templates.validateTemplates(entities.values(), logFailures=True)

        # Rerun but with a failure
        entities["pvi"] = self.makeDatasetRef("pvi", storageClassName="StorageClassX", dataId={"band": "i"})
        with self.assertRaises(FileTemplateValidationError):
            with self.assertLogs(level="FATAL"):
                templates.validateTemplates(entities.values(), logFailures=True)


if __name__ == "__main__":
    unittest.main()
