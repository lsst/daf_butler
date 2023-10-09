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


import logging
import unittest
from contextlib import contextmanager

import click
from lsst.daf.butler.cli.progress import ClickProgressHandler
from lsst.daf.butler.cli.utils import clickResultMsg
from lsst.daf.butler.progress import Progress, ProgressHandler


class MockProgressBar:
    """Mock implementation of `ProgressBar` that remembers the status it
    would report in a list.

    Both the initial 0 and the end-of-iterable size are reported.

    Parameters
    ----------
    iterable : `Iterable`
        Iterable to wrap, or `None`.
    total : `int` or `None`
        Total value passed at progress-bar construction.
    """

    def __init__(self, iterable, total):
        self._iterable = iterable
        self._current = 0
        self.reported = [self._current]
        self.total = total
        MockProgressBar.last = self

    last = None
    """Last instance of this class that was constructed, for test code that
    cannot access it directly via other means.
    """

    def __iter__(self):
        for element in self._iterable:
            yield element
            self._current += 1
            self.reported.append(self._current)

    def update(self, n: int = 1) -> None:
        self._current += n
        self.reported.append(self._current)


class MockProgressHandler(ProgressHandler):
    """A `ProgressHandler` implementation that returns `MockProgressBar`
    instances.
    """

    @contextmanager
    def get_progress_bar(self, iterable, desc, total, level):
        yield MockProgressBar(iterable, total=total)


class ClickProgressHandlerTestCase(unittest.TestCase):
    """Test enabling and disabling progress in click commands.

    It looks like click's testing harness doesn't ever actually let its
    progress bar generate output, so the best we can do is check that using it
    doesn't raise exceptions, and see if it looks like we're doing something
    based on what our own progress-object state is.
    """

    def setUp(self):
        # Set up a mock handler by default.  Tests of click behavior will
        # rely on this when they check that inside a click command we never
        # end up with that mock.
        self.logger = logging.getLogger("test_progress")
        self.logger.setLevel(logging.INFO)
        Progress.set_handler(MockProgressHandler())
        self.runner = click.testing.CliRunner()

    def tearDown(self):
        MockProgressHandler.last = None
        Progress.set_handler(None)
        self.logger.setLevel(logging.NOTSET)

    def get_cmd(self, level, enabled):
        """Return a click command that uses a progress bar and tests that it
        is or not enabled, as given.
        """

        @click.command()
        @ClickProgressHandler.option
        def cmd(progress):
            p = Progress("test_progress", level=level)
            with p.bar(range(5), desc="testing!") as bar:
                self.assertFalse(isinstance(bar, MockProgressBar))
                r = list(bar)
            self.assertEqual(r, list(range(5)))
            self.assertEqual(enabled, p.is_enabled())

        return cmd

    def test_click_disabled_by_default(self):
        """Test that progress is disabled by default in click commands."""
        result = self.runner.invoke(
            self.get_cmd(logging.INFO, enabled=False),
            [],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

    def test_click_enabled(self):
        """Test turning on progress in click commands."""
        result = self.runner.invoke(
            self.get_cmd(logging.INFO, enabled=True),
            ["--progress"],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

    def test_click_disabled_globally(self):
        """Test turning on progress in click commands."""
        result = self.runner.invoke(
            self.get_cmd(logging.INFO, enabled=False),
            ["--no-progress"],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

    def test_click_disabled_by_log_level(self):
        """Test that progress reports below the current log level are disabled,
        even if progress is globally enabled.
        """
        result = self.runner.invoke(
            self.get_cmd(logging.DEBUG, enabled=False),
            ["--progress"],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))


class MockedProgressHandlerTestCase(unittest.TestCase):
    """Test that the interface layer for progress reporting works by using
    mock handler and progress bar objects.
    """

    def setUp(self):
        self.logger = logging.getLogger("test_progress")
        self.logger.setLevel(logging.INFO)
        Progress.set_handler(MockProgressHandler())
        self.progress = Progress("test_progress")

    def tearDown(self):
        MockProgressHandler.last = None
        Progress.set_handler(None)
        self.logger.setLevel(logging.NOTSET)

    def test_bar_iterable(self):
        """Test using `Progress.bar` to wrap an iterable."""
        iterable = list(range(5))
        with self.progress.bar(iterable) as bar:
            r = list(bar)
        self.assertEqual(r, iterable)
        self.assertEqual(iterable + [len(iterable)], bar.reported)

    def test_bar_update(self):
        """Test using `Progress.bar` with manual updates."""
        with self.progress.bar(total=10) as bar:
            for _ in range(5):
                bar.update(2)
        self.assertEqual(list(range(0, 12, 2)), bar.reported)

    def test_iter_chunks_fully_sized(self):
        """Test using `Progress.iter_chunks` with a sized iterable of sized
        chunks.
        """
        iterable = [list(range(2)), list(range(3))]
        seen = []
        for chunk in self.progress.iter_chunks(iterable):
            seen.extend(chunk)
        self.assertEqual(seen, iterable[0] + iterable[1])
        self.assertEqual(MockProgressBar.last.reported, [0, 2, 5])
        self.assertEqual(MockProgressBar.last.total, 5)

    def test_iter_chunks_with_total(self):
        """Test using `Progress.iter_chunks` with total provided and
        sized chunks.
        """
        iterable = [list(range(2)), list(range(3))]
        seen = []
        for chunk in self.progress.iter_chunks(iter(iterable), total=5):
            seen.extend(chunk)
        self.assertEqual(seen, iterable[0] + iterable[1])
        self.assertEqual(MockProgressBar.last.reported, [0, 2, 5])
        self.assertEqual(MockProgressBar.last.total, 5)

    def test_iter_chunks_total_false(self):
        """Test using `Progress.iter_chunks` with total=False and non-sized
        chunks.  This should display progress with the number of
        chunks.
        """
        iterable = [iter(range(2)), iter(range(3))]
        seen = []
        for chunk in self.progress.iter_chunks(iterable, total=False):
            seen.extend(chunk)
        self.assertEqual(seen, list(range(2)) + list(range(3)))
        self.assertEqual(MockProgressBar.last.reported, [0, 1, 2])
        self.assertEqual(MockProgressBar.last.total, 2)

    def test_iter_chunks_not_sized(self):
        """Test using `Progress.iter_chunks` with an unsized iterable."""
        iterable = [iter(range(2)), iter(range(3))]
        seen = []
        for chunk in self.progress.iter_chunks(iter(iterable)):
            seen.extend(chunk)
        self.assertEqual(seen, list(range(2)) + list(range(3)))
        self.assertEqual(MockProgressBar.last.reported, [0, 1, 2])
        self.assertEqual(MockProgressBar.last.total, None)

    def test_iter_item_chunks_fully_sized(self):
        """Test using `Progress.iter_item_chunks` with a sized iterable of
        sized chunks.
        """
        mapping = {"x": list(range(2)), "y": list(range(3))}
        seen = {}
        for key, chunk in self.progress.iter_item_chunks(mapping.items()):
            seen[key] = chunk
        self.assertEqual(seen, mapping)
        self.assertEqual(MockProgressBar.last.reported, [0, 2, 5])
        self.assertEqual(MockProgressBar.last.total, 5)

    def test_iter_item_chunks_with_total(self):
        """Test using `Progress.iter_item_chunks` with total provided and
        sized chunks.
        """
        mapping = {"x": list(range(2)), "y": list(range(3))}
        seen = {}
        for key, chunk in self.progress.iter_item_chunks(iter(mapping.items()), total=5):
            seen[key] = chunk
        self.assertEqual(seen, mapping)
        self.assertEqual(MockProgressBar.last.reported, [0, 2, 5])
        self.assertEqual(MockProgressBar.last.total, 5)

    def test_iter_item_chunks_total_false(self):
        """Test using `Progress.iter_item_chunks` with total=False and
        non-sized chunks.  This should display progress with the number of
        chunks.
        """
        mapping = {"x": iter(range(2)), "y": iter(range(3))}
        seen = {}
        for key, chunk in self.progress.iter_item_chunks(mapping.items(), total=False):
            seen[key] = list(chunk)
        self.assertEqual(seen, {"x": list(range(2)), "y": list(range(3))})
        self.assertEqual(MockProgressBar.last.reported, [0, 1, 2])
        self.assertEqual(MockProgressBar.last.total, 2)

    def test_iter_item_chunks_not_sized(self):
        """Test using `Progress.iter_item_chunks` with an unsized iterable of
        non-sized chunks.
        """
        mapping = {"x": iter(range(2)), "y": iter(range(3))}
        seen = {}
        for key, chunk in self.progress.iter_item_chunks(iter(mapping.items())):
            seen[key] = list(chunk)
        self.assertEqual(seen, {"x": list(range(2)), "y": list(range(3))})
        self.assertEqual(MockProgressBar.last.reported, [0, 1, 2])
        self.assertEqual(MockProgressBar.last.total, None)


if __name__ == "__main__":
    unittest.main()
