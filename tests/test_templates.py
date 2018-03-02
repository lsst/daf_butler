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

from lsst.daf.butler.core.fileTemplates import FileTemplate
from lsst.daf.butler.core.dataUnits import DataUnits


class TestFileTemplates(unittest.TestCase):
    """Test creation of paths from templates."""
    def setUp(self):
        self.du = DataUnits({"visit": 52, "filter": "U"})

    def assertTemplate(self, template, answer, dataUnits, **kwargs):
        fileTmpl = FileTemplate(template)
        path = fileTmpl.format(dataUnits, **kwargs)
        self.assertEqual(path, answer)

    def testBasic(self):
        tmplstr = "{datasetType}/{visit:05d}/{filter}"
        self.assertTemplate(tmplstr,
                            tmplstr.format(datasetType="calexp", **self.du.units),
                            self.du, datasetType="calexp")

    def testOptional(self):
        """Optional units in templates."""
        tmplstr = "{datasetType}/v{visit:05d}_f{filter:?}"
        self.assertTemplate(tmplstr, "calexp/v00052_fU",
                            self.du, datasetType="calexp")

        du = DataUnits({"visit": 48, "tract": 265})
        self.assertTemplate(tmplstr, "calexp/v00048",
                            du, datasetType="calexp")

        # Ensure that this returns a relative path even if the first field
        # is optional
        tmplstr = "{datasetType:?}/{visit:?}/f{filter}"
        self.assertTemplate(tmplstr, "52/fU", self.du)

        # Ensure that // from optionals are converted to singles
        tmplstr = "{datasetType}/{patch:?}/{tract:?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/fU", self.du, datasetType="calexp")

        # Optionals with some text between fields
        tmplstr = "{datasetType}/p{patch:?}_t{tract:?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/p/fU", self.du, datasetType="calexp")
        tmplstr = "{datasetType}/p{patch:?}_t{visit:04d?}/f{filter}"
        self.assertTemplate(tmplstr, "calexp/p_t0052/fU", self.du, datasetType="calexp")

    def testComponent(self):
        """Test handling of components in templates."""

        tmplstr = "c_{component}_v{visit}"
        self.assertTemplate(tmplstr, "c_output_v52", self.du, datasetType="metric.output")
        self.assertTemplate(tmplstr, "c_output_v52", self.du, component="output")

        tmplstr = "{component:?}_{visit}"
        self.assertTemplate(tmplstr, "_52", self.du)
        self.assertTemplate(tmplstr, "output_52", self.du, datasetType="metric.output")
        self.assertTemplate(tmplstr, "maskedimage.variance_52", self.du,
                            datasetType="calexp.maskedimage.variance")
        self.assertTemplate(tmplstr, "output_52", self.du, component="output")

        # Providing a component but not using it
        tmplstr = "{datasetType}/v{visit:05d}"
        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", self.du, datasetType="calexp", component="wcs")
        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", self.du, datasetType="calexp.wcs")


if __name__ == "__main__":
    unittest.main()
