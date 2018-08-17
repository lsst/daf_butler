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

import unittest

from lsst.daf.butler import DatasetType, DatasetRef, FileTemplate, StorageClass


class TestFileTemplates(unittest.TestCase):
    """Test creation of paths from templates."""

    def makeDatasetRef(self, datasetTypeName, dataId=None):
        """Make a simple DatasetRef"""
        if dataId is None:
            dataId = self.dataId
        if datasetTypeName not in self.datasetTypes:
            self.datasetTypes[datasetTypeName] = DatasetType(datasetTypeName, list(dataId.keys()),
                                                             StorageClass(None))
        datasetType = self.datasetTypes[datasetTypeName]
        return DatasetRef(datasetType, dataId)

    def setUp(self):
        self.dataId = {"visit": 52, "filter": "U"}
        self.datasetTypes = {}

    def assertTemplate(self, template, answer, ref):
        fileTmpl = FileTemplate(template)
        path = fileTmpl.format(ref)
        self.assertEqual(path, answer)

    def testBasic(self):
        tmplstr = "{datasetType}/{visit:05d}/{filter}"
        self.assertTemplate(tmplstr,
                            "calexp/00052/U",
                            self.makeDatasetRef("calexp"))
        tmplstr = "{datasetType}/{visit:05d}/{filter}-trail"
        self.assertTemplate(tmplstr,
                            "calexp/00052/U-trail",
                            self.makeDatasetRef("calexp"))

    def testOptional(self):
        """Optional units in templates."""
        ref = self.makeDatasetRef("calexp")
        tmplstr = "{datasetType}/v{visit:05d}_f{filter:?}"
        self.assertTemplate(tmplstr, "calexp/v00052_fU",
                            self.makeDatasetRef("calexp"))

        du = {"visit": 48, "tract": 265}
        self.assertTemplate(tmplstr, "calexpT/v00048",
                            self.makeDatasetRef("calexpT", du))

        # Ensure that this returns a relative path even if the first field
        # is optional
        tmplstr = "{tract:?}/{visit:?}/f{filter}"
        self.assertTemplate(tmplstr, "52/fU", ref)

        # Ensure that // from optionals are converted to singles
        tmplstr = "{datasetType}/{patch:?}/{tract:?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/fU", ref)

        # Optionals with some text between fields
        tmplstr = "{datasetType}/p{patch:?}_t{tract:?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/p/fU", ref)
        tmplstr = "{datasetType}/p{patch:?}_t{visit:04d?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/p_t0052/fU", ref)

    def testComponent(self):
        """Test handling of components in templates."""
        refMetricOutput = self.makeDatasetRef("metric.output")
        refMetric = self.makeDatasetRef("metric")
        refMaskedImage = self.makeDatasetRef("calexp.maskedimage.variance")
        refWcs = self.makeDatasetRef("calexp.wcs")

        tmplstr = "c_{component}_v{visit}"
        self.assertTemplate(tmplstr, "c_output_v52", refMetricOutput)

        tmplstr = "{component:?}_{visit}"
        self.assertTemplate(tmplstr, "_52", refMetric)
        self.assertTemplate(tmplstr, "output_52", refMetricOutput)
        self.assertTemplate(tmplstr, "maskedimage.variance_52", refMaskedImage)
        self.assertTemplate(tmplstr, "output_52", refMetricOutput)

        # Providing a component but not using it
        tmplstr = "{datasetType}/v{visit:05d}"
        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", refWcs)


if __name__ == "__main__":
    unittest.main()
