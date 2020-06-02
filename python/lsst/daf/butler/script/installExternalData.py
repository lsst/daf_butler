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


import os
from lsst.daf.persistence import Butler


def linkFile(source, butler, datasetType, dataId):
    """Link a file in the butler to some source outside

    We use links to avoid copying files unnecessarily, and because the
    ci_hsc_gen3 ``exportExternalData`` script expects everything to live
    in testdata_ci_hsc still.

    Parameters
    ----------
    source : `str`
        Filename for source data.
    butler : `lsst.daf.peristence.Butler`
        Data butler.
    datasetType : `str`
        Dataset type in butler.
    dataId : `dict`
        Data identifiers.
    """
    target = butler.get(f"{datasetType}_filename", dataId)[0]
    dirName = os.path.dirname(target)
    if not os.path.isdir(dirName):
        os.makedirs(dirName)
    os.symlink(os.path.relpath(source, dirName), target)


def installJointcal(source, butler, tract, visitCcdList):
    """Install jointcal data

    The jointcal data is read from the nominated ``source``, and written using
    the butler.

    Parameters
    ----------
    source : `str`
        Path to the source jointcal data.
    butler : `lsst.daf.persistence.Butler`
        Data butler.
    tract : `int`
        Tract identifier.
    visitCcdList : iterable of pair of `int`
        [[`visit`, `ccd`], ...]
    """
    for visit, ccd in visitCcdList:
        suffix = f"{visit:07d}-{ccd:03d}.fits"
        dataId = dict(tract=tract, visit=visit, ccd=ccd)
        linkFile(os.path.join(source, f"jointcal_photoCalib-{suffix}"), butler, "jointcal_photoCalib", dataId)
        linkFile(os.path.join(source, f"jointcal_wcs-{suffix}"), butler, "jointcal_wcs", dataId)


def installExternalData(repo, source, tract, visit_ccd):
    butler = Butler(repo)
    installJointcal(source, butler, tract, visit_ccd)
