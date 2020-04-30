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

"""Unit tests for the 'repo' shared CLI option.
"""

import click
import click.testing
import unittest

from lsst.daf.butler.cli.opt.repo import repo_option


@click.command()
@repo_option(required=True)
def repoRequired(repo):
    pass


@click.command()
@repo_option()  # required default val is False
def repoNotRequired(repo):
    pass


@click.command()
@repo_option(help="custom help text")
def repoWithHelpText(repo):
    pass


class Suite(unittest.TestCase):

    def testRequired_provided(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(repoRequired, ["--repo", "location"])
        self.assertEqual(result.exit_code, 0)

    def testRequired_notProvided(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(repoRequired)
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Missing option "--repo"', result.output)

    def testNotRequired_provided(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(repoNotRequired, ["--repo", "location"])
        self.assertEqual(result.exit_code, 0)

    def testNotRequired_notProvided(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(repoNotRequired)
        self.assertEqual(result.exit_code, 0)

    def testHelp(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(repoWithHelpText, "--help")
        self.assertIn("custom help text", result.output)


if __name__ == "__main__":
    unittest.main()
