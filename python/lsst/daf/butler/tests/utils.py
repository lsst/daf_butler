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


from astropy.table import Table as AstropyTable
from astropy.utils.diff import report_diff_values
import io


class ButlerTestHelper:
    """Mixin with helpers for unit tests."""

    def assertAstropyTablesEqual(self, tables, expectedTables):
        """Verify that a list of astropy tables matches a list of expected
        astropy tables.

        Parameters
        ----------
        tables : `astropy.table.Table` or iterable [`astropy.table.Table`]
            The table or tables that should match the expected tables.
        expectedTables : `astropy.table.Table`
                         or iterable [`astropy.table.Table`]
            The tables with expected values to which the tables under test will
            be compared.
        """
        # If a single table is passed in for tables or expectedTables, put it
        # in a list.
        if isinstance(tables, AstropyTable):
            tables = [tables]
        if isinstance(expectedTables, AstropyTable):
            expectedTables = [expectedTables]
        diff = io.StringIO()
        self.assertEqual(len(tables), len(expectedTables))
        for table, expected in zip(tables, expectedTables):
            # Assert that we are testing what we think we are testing:
            self.assertIsInstance(table, AstropyTable)
            self.assertIsInstance(expected, AstropyTable)
            # Assert that they match:
            self.assertTrue(report_diff_values(table, expected, fileobj=diff), msg="\n" + diff.getvalue())
