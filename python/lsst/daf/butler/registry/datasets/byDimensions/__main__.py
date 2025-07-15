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

__all__ = ()

import os
import time
from collections.abc import Iterable
from typing import Literal

import click

from ...._butler import Butler
from ....cli.cliLog import CliLog
from ....cli.opt import (
    log_file_option,
    log_label_option,
    log_level_option,
    log_tty_option,
    long_log_option,
)


@click.command()
@click.argument("repo")
@click.argument("collection", nargs=-1)
@click.option("--dataset-type", "-d", multiple=True)
@click.option("--mode", type=click.Choice(["inexact", "synthetic", "hybrid"], case_sensitive=False))
@click.option("--explain", is_flag=True)
@log_level_option()
@long_log_option()
@log_file_option()
@log_tty_option()
@log_label_option()
def benchmark(
    *,
    repo: str,
    collection: Iterable[str],
    dataset_type: Iterable[str],
    mode: Literal["inexact", "synthetic", "hybrid"],
    explain: bool,
    long_log: bool,
    log_tty: bool,
    log_file: tuple[str, ...],
    log_label: dict[str, str] | None,
) -> None:
    CliLog.initLog(longlog=long_log, log_file=log_file, log_tty=log_tty, log_label=log_label)
    butler = Butler.from_config(repo)
    old_debug_queries = ""
    if explain:
        old_debug_queries = os.environ["DAF_BUTLER_DEBUG_QUERIES"]
        os.environ["DAF_BUTLER_DEBUG_QUERIES"] = "1"
    mgr = butler.registry._registry._managers.datasets
    t0 = time.perf_counter()
    result = mgr.fetch_summaries(collection, dataset_type, mode=mode)
    t1 = time.perf_counter()
    if explain:
        os.environ["DAF_BUTLER_DEBUG_QUERIES"] = old_debug_queries
    print(f"Fetched {len(result)} summaries in {t1 - t0} seconds.")


if __name__ == "__main__":
    benchmark()
