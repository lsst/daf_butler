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

"""Unit tests for daf_butler command line interface commands.
"""

import click
import click.testing
import os
import unittest

from lsst.daf.butler.script import butler


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ButlerCliTestSuite(unittest.TestCase):
    def testCreate(self):
        '''Test creating a repostiry.'''
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ['create', 'here'])
            self.assertEqual(result.exit_code, 0)

    def testCreate_outfile(self):
        '''Test creating a repository and specify an outfile location.'''
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ['create', 'here', '--outfile=there'])
            self.assertEqual(result.exit_code, 0)
            self.assertTrue(os.path.exists('there'))  # verify the separate config file was made
            with open('there', 'r') as f:
                contents = ' '.join(f.readlines())
                # verify a few expected strings are in the config file:
                self.assertTrue('datastore' in contents)
                self.assertTrue('PosixDatastore' in contents)
                self.assertTrue('root' in contents)

    # This test passes when I run this test script directly but fails when I
    # run the scons build. It needs investigation
    # def testCreate_verbose(self):
    #     '''Test creating a repository, with verbose output.'''
    #     runner = click.testing.CliRunner()
    #     with runner.isolated_filesystem():
    #         result = runner.invoke(butler.cli, ['create', 'here', '-v'])
    #         self.assertEqual(result.exit_code, 0)
    #         verify at least one debug statement printed:
    #         self.assertTrue('DEBUG' in result.stdout)

    def testDumpConfig(self):
        '''Test dumping the config to stdout.'''
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ['create', 'here'])
            self.assertEqual(result.exit_code, 0)

            # test dumping to stdout:
            result = runner.invoke(butler.cli, ['dump-config', 'here'])
            self.assertEqual(result.exit_code, 0)
            # check for some expected keywords:
            self.assertTrue('composites' in result.stdout)
            self.assertTrue('datastore' in result.stdout)
            self.assertTrue('storageClasses' in result.stdout)

    def testDumpConfig_file(self):
        '''test dumping the config to a file.'''
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ['create', 'here'])
            self.assertEqual(result.exit_code, 0)
            result = runner.invoke(butler.cli, ['dump-config', 'here', '--file=there'])
            self.assertEqual(result.exit_code, 0)
            # check for some expected keywords:
            with open('there', 'r') as f:
                contents = ' '.join(f.readlines())
                self.assertTrue('composites' in contents)
                self.assertTrue('datastore' in contents)
                self.assertTrue('storageClasses' in contents)

    # this fails intermitently
    # @unittest.expectedFailure
    # # For some reason, result.stdout (and result.output) don't have the
    # # messages logged in this case. I've verified the DEBUG statements *do*
    # # appear when running the script from the command line (instead of from
    # # the CliRunner as we do here).
    # def testDumpConfig_verbose(self):
    #     ''''Test dumping the config to the command line, with verbose output.
    #     '''
    #     runner = click.testing.CliRunner()
    #     with runner.isolated_filesystem():
    #         result = runner.invoke(butler.cli, ['create', 'here'])
    #         self.assertEqual(result.exit_code, 0)
    #         result = runner.invoke(butler.cli, ['dump-config', 'here', '-v'])
    #         self.assertEqual(result.exit_code, 0)
    #         #  verify at least one debug statement printed:
    #         self.assertTrue('DEBUG' in result.stdout)

    def testValidateConfig(self):
        '''Test validating a valid config.'''
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ['create', 'here'])
            self.assertEqual(result.exit_code, 0)
            # verify the just-created repo validates without error, resulting
            # in no output.
            result = runner.invoke(butler.cli, ['validate-config', 'here'])
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, '')


if __name__ == "__main__":
    unittest.main()
