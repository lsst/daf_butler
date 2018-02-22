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
        tmplstr = "{datasettype}/{visit:05d}/{filter}"
        self.assertTemplate(tmplstr,
                            tmplstr.format(datasettype="calexp", **self.du.units),
                            self.du, datasettype="calexp")

    def testOptional(self):
        tmplstr = "{datasettype}.{component:?}/v{visit:05d}_f{filter}"
        self.assertTemplate(tmplstr, "calexp.wcs/v00052_fU",
                            self.du, datasettype="calexp.wcs")
        self.assertTemplate(tmplstr, "calexp.psf/v00052_fU",
                            self.du, datasettype="calexp", component="psf")

        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", self.du, component="wcs")

        # Ensure that this returns a relative path even if the first field
        # is optional
        tmplstr = "{datasettype:?}/{visit:?}/f{filter}"
        self.assertTemplate(tmplstr, "52/fU", self.du)

    def testComponent(self):
        tmplstr = "{datasettype}/v{visit:05d}"
        self.assertTemplate(tmplstr, "calexp/v00052", self.du, datasettype="calexp")

        with self.assertRaises(KeyError):
            self.assertTemplate(tmplstr, "", self.du, datasettype="calexp", component="wcs")

        tmplstr = "{component:?}_{visit}"
        self.assertTemplate(tmplstr, "_52", self.du)
        self.assertTemplate(tmplstr, "output_52", self.du, datasettype="metric.output")


if __name__ == "__main__":
    unittest.main()
