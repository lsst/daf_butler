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

import logging
from typing import Any

from fastapi import Depends, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from lsst.daf.butler import Butler

BUTLER_ROOT = "ci_hsc_gen3/DATA"

log = logging.getLogger("excalibur")

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


GLOBAL_READWRITE_BUTLER: Butler | None = None
GLOBAL_READONLY_BUTLER: Butler | None = None


def _make_global_butler() -> None:
    global GLOBAL_READONLY_BUTLER, GLOBAL_READWRITE_BUTLER
    if GLOBAL_READONLY_BUTLER is None:
        GLOBAL_READONLY_BUTLER = Butler.from_config(BUTLER_ROOT, writeable=False)
    if GLOBAL_READWRITE_BUTLER is None:
        GLOBAL_READWRITE_BUTLER = Butler.from_config(BUTLER_ROOT, writeable=True)


def butler_readonly_dependency() -> Butler:
    """Return global read-only butler."""
    _make_global_butler()
    return Butler.from_config(butler=GLOBAL_READONLY_BUTLER)


def butler_readwrite_dependency() -> Butler:
    """Return read-write butler."""
    _make_global_butler()
    return Butler.from_config(butler=GLOBAL_READWRITE_BUTLER)


@app.get("/butler/")
def read_root() -> str:
    """Return message when accessing the root URL."""
    return "Welcome to Excalibur... aka your Butler Server"


@app.get("/butler/v1/universe", response_model=dict[str, Any])
def get_dimension_universe(butler: Butler = Depends(butler_readonly_dependency)) -> dict[str, Any]:
    """Allow remote client to get dimensions definition."""
    return butler.dimensions.dimensionConfig.toDict()
