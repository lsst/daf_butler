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

from __future__ import annotations

import os
import unittest

from lsst.daf.butler import Butler, Config
from lsst.daf.butler.tests._repo_template_cache import (
    clear_repo_template_cache,
    make_repo_for_test,
    template_cache_stats,
)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class RepoTemplateCacheTestCase(unittest.TestCase):
    """Tests for the test repository template cache."""

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        clear_repo_template_cache()

    def tearDown(self) -> None:
        removeTestTempDir(self.root)
        clear_repo_template_cache()

    def _config(self) -> Config:
        config = Config()
        config["datastore", "cls"] = "lsst.daf.butler.datastores.inMemoryDatastore.InMemoryDatastore"
        config["registry", "db"] = "sqlite:///<butlerRoot>/gen3.sqlite3"
        return config

    def test_second_call_is_served_from_cache(self) -> None:
        """An identical configuration builds one template and copies it."""
        first = os.path.join(self.root, "one")
        second = os.path.join(self.root, "two")
        make_repo_for_test(first, config=self._config(), forceConfigRoot=False)
        make_repo_for_test(second, config=self._config(), forceConfigRoot=False)
        stats = template_cache_stats()
        self.assertEqual(stats.templates, 1)
        self.assertEqual(stats.served, 2)

    def test_returned_config_points_at_the_caller_root(self) -> None:
        """The returned Config must describe the copy, not the template."""
        first = os.path.join(self.root, "one")
        second = os.path.join(self.root, "two")
        make_repo_for_test(first, config=self._config(), forceConfigRoot=False)
        returned = make_repo_for_test(second, config=self._config(), forceConfigRoot=False)
        butler = Butler.from_config(returned, writeable=True)
        self.assertIn(second, str(butler._registry._db.filename))

    def test_copied_repo_is_usable_and_independent(self) -> None:
        """Two repos from one template do not share state."""
        first = os.path.join(self.root, "one")
        second = os.path.join(self.root, "two")
        make_repo_for_test(first, config=self._config(), forceConfigRoot=False)
        make_repo_for_test(second, config=self._config(), forceConfigRoot=False)
        b1 = Butler.from_config(first, writeable=True, run="r1")
        b2 = Butler.from_config(second, writeable=True, run="r2")
        b1.registry.registerRun("only_in_first")
        self.assertNotIn("only_in_first", set(b2.registry.queryCollections()))

    def test_datastore_config_shares_one_database(self) -> None:
        """Datastore settings do not reach the database, so it is reused."""
        other = self._config()
        other["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
        other["datastore", "checksum"] = False
        make_repo_for_test(os.path.join(self.root, "one"), config=self._config(), forceConfigRoot=False)
        make_repo_for_test(os.path.join(self.root, "two"), config=other, forceConfigRoot=False)
        stats = template_cache_stats()
        self.assertEqual(stats.templates, 1)
        self.assertEqual(stats.reused_database, 1)

    def test_each_repo_gets_its_own_butler_yaml(self) -> None:
        """Sharing a database must not share the rest of the configuration."""
        other = self._config()
        other["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
        first = os.path.join(self.root, "one")
        second = os.path.join(self.root, "two")
        make_repo_for_test(first, config=self._config(), forceConfigRoot=False)
        make_repo_for_test(second, config=other, forceConfigRoot=False)
        self.assertIn("inMemoryDatastore", Config(os.path.join(first, "butler.yaml"))["datastore", "cls"])
        self.assertIn("fileDatastore", Config(os.path.join(second, "butler.yaml"))["datastore", "cls"])

    def _files_under(self, directory: str) -> set[str]:
        """Return every file below a directory, relative to it."""
        found = set()
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                found.add(os.path.relpath(os.path.join(dirpath, filename), directory))
        return found

    def test_root_with_uri_metacharacters(self) -> None:
        """A root holding `?` or `#` must land where makeRepo puts it.

        Such a root is mangled during creation, because the tag replacement
        drops a URI fragment and the database connection string is then parsed
        as a URI. The helper has to follow that resolution rather than
        predict a path of its own.
        """
        helper_parent = os.path.join(self.root, "helper")
        direct_parent = os.path.join(self.root, "direct")
        subdir = "sub?#dir"
        make_repo_for_test(os.path.join(helper_parent, subdir), config=self._config(), forceConfigRoot=False)
        Butler.makeRepo(os.path.join(direct_parent, subdir), config=self._config(), forceConfigRoot=False)
        self.assertEqual(self._files_under(helper_parent), self._files_under(direct_parent))

        # Producing the right files is not enough on its own, because failing
        # to locate the database would also leave the repository correct but
        # uncached. A second such root has to reuse the first one's database,
        # which it can only do if the location was resolved rather than
        # guessed.
        second_parent = os.path.join(self.root, "second")
        make_repo_for_test(os.path.join(second_parent, subdir), config=self._config(), forceConfigRoot=False)
        stats = template_cache_stats()
        self.assertEqual(stats.templates, 1)
        self.assertEqual(stats.reused_database, 1)
        self.assertEqual(self._files_under(second_parent), self._files_under(direct_parent))

    def test_obscore_config_survives_a_cache_hit(self) -> None:
        """Obscore settings are stripped from ``butler.yaml`` and kept in the
        registry, so a copy cannot recover them from the file.
        """
        config = self._config()
        config["registry", "managers", "obscore"] = {
            "cls": "lsst.daf.butler.registry.obscore.ObsCoreLiveTableManager",
            "config": Config(os.path.join(TESTDIR, "config", "basic", "obscore.yaml")).toDict(),
        }
        second = os.path.join(self.root, "two")
        make_repo_for_test(os.path.join(self.root, "one"), config=config, forceConfigRoot=False)
        returned = make_repo_for_test(second, config=config, forceConfigRoot=False)
        self.assertEqual(template_cache_stats().reused_config, 1)
        obscore_config = ("registry", "managers", "obscore", "config")
        self.assertNotIn(obscore_config, Config(os.path.join(second, "butler.yaml")))
        self.assertIn(obscore_config, returned)
        with Butler.from_config(second, writeable=True) as butler:
            self.assertIsNotNone(butler._registry.obsCoreTableManager)

    def test_existing_config_is_not_overwritten(self) -> None:
        """A copy must refuse an occupied root, as makeRepo does."""
        second = os.path.join(self.root, "two")
        make_repo_for_test(os.path.join(self.root, "one"), config=self._config(), forceConfigRoot=False)
        make_repo_for_test(second, config=self._config(), forceConfigRoot=False)
        self.assertEqual(template_cache_stats().reused_config, 1)
        with self.assertRaises(FileExistsError):
            make_repo_for_test(second, config=self._config(), forceConfigRoot=False)

    def test_rewritten_config_file_is_not_reused(self) -> None:
        """A configuration named by path is identified by its contents."""
        path = os.path.join(self.root, "repo.yaml")
        self._config().dumpToUri(path)
        make_repo_for_test(os.path.join(self.root, "one"), config=path, forceConfigRoot=False)
        changed = self._config()
        changed["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
        changed.dumpToUri(path, overwrite=True)
        second = os.path.join(self.root, "two")
        make_repo_for_test(second, config=path, forceConfigRoot=False)
        self.assertIn("fileDatastore", Config(os.path.join(second, "butler.yaml"))["datastore", "cls"])
        self.assertEqual(template_cache_stats().reused_config, 0)

    def test_different_dimensions_build_a_second_database(self) -> None:
        """The dimension universe does reach the database, so it keys."""
        path = os.path.join(TESTDIR, "config", "dimensions", "dimensions1.yaml")
        make_repo_for_test(os.path.join(self.root, "one"), config=self._config(), forceConfigRoot=False)
        make_repo_for_test(
            os.path.join(self.root, "two"),
            config=self._config(),
            dimensionConfig=path,
            forceConfigRoot=False,
        )
        self.assertEqual(template_cache_stats().templates, 2)

    def test_outfile_bypasses_the_cache(self) -> None:
        """Outfile writes the config elsewhere, so it cannot be cached."""
        make_repo_for_test(os.path.join(self.root, "one"), config=self._config(), forceConfigRoot=False)
        make_repo_for_test(
            os.path.join(self.root, "two"),
            config=self._config(),
            forceConfigRoot=False,
            outfile=os.path.join(self.root, "out.yaml"),
        )
        self.assertEqual(template_cache_stats().bypassed, 1)

    def test_non_sqlite_registry_bypasses_the_cache(self) -> None:
        """A client/server registry lives outside the copied directory."""
        config = self._config()
        config["registry", "db"] = "postgresql://example.invalid/butler"
        config["registry", "namespace"] = "namespace_deadbeef"
        with self.assertRaises(Exception):
            # The database does not exist, so creation fails. What matters is
            # that the attempt bypassed the cache rather than building and
            # retaining a single-use template.
            make_repo_for_test(os.path.join(self.root, "pg"), config=config, forceConfigRoot=False)
        stats = template_cache_stats()
        self.assertEqual(stats.bypassed, 1)
        self.assertEqual(stats.templates, 0)

    def test_standalone_and_overwrite_bypass_the_cache(self) -> None:
        """Standalone and overwrite change what makeRepo writes."""
        make_repo_for_test(os.path.join(self.root, "a"), config=self._config(), standalone=True)
        make_repo_for_test(os.path.join(self.root, "b"), config=self._config(), overwrite=True)
        self.assertEqual(template_cache_stats().bypassed, 2)


if __name__ == "__main__":
    unittest.main()
