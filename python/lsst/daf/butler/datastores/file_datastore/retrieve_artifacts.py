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

__all__ = ("determine_destination_for_retrieved_artifact",)

from lsst.resources import ResourcePath, ResourcePathExpression


def determine_destination_for_retrieved_artifact(
    destination_directory: ResourcePath, source_path: ResourcePath, preserve_path: bool
) -> ResourcePath:
    """Determine destination path for an artifact retrieved from a datastore.

    Parameters
    ----------
    destination_directory : `ResourcePath`
        Path to the output directory where file will be stored.
    source_path : `ResourcePath`
        Path to the source file to be transferred.  This may be relative to the
        datastore root, or an absolute path.
    preserve_path : `bool`, optional
        If `True` the full path of the artifact within the datastore
        is preserved. If `False` the final file component of the path
        is used.

    Returns
    -------
    destination_uri : `~lsst.resources.ResourcePath`
        Absolute path to the output destination.
    """
    destination_directory = destination_directory.abspath()

    target_path: ResourcePathExpression
    if preserve_path:
        target_path = source_path
        if target_path.isabs():
            # This is an absolute path to an external file.
            # Use the full path.
            target_path = target_path.relativeToPathRoot
    else:
        target_path = source_path.basename()

    target_uri = destination_directory.join(target_path).abspath()
    if target_uri.relative_to(destination_directory) is None:
        raise ValueError(f"File path attempts to escape destination directory: '{source_path}'")
    return target_uri
