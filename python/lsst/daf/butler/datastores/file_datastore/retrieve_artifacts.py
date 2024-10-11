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

__all__ = ("determine_destination_for_retrieved_artifact", "retrieve_and_zip", "ZipIndex")

import tempfile
import uuid
import zipfile
from collections.abc import Callable, Iterable

from lsst.daf.butler import DatasetId, DatasetIdFactory, DatasetRef, SerializedDatasetRef
from lsst.daf.butler.datastore.stored_file_info import SerializedStoredFileInfo, StoredFileInfo
from lsst.resources import ResourcePath, ResourcePathExpression
from pydantic import BaseModel


class ZipIndex(BaseModel):
    """Index of a Zip file of Butler datasets.

    A file can be associated with multiple butler datasets and a single
    butler dataset can be associated with multiple files. This model
    provides the necessary information for ingesting back into a Butler
    file datastore.
    """

    refs: list[SerializedDatasetRef]
    """The Butler datasets stored in the Zip file."""
    # Can have multiple refs associated with a single file.
    ref_map: dict[str, list[uuid.UUID]]
    """Mapping of Zip member to one or more dataset UUIDs."""
    info_map: dict[str, SerializedStoredFileInfo]
    """Mapping of each Zip member to the associated datastore record."""


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


def retrieve_and_zip(
    refs: Iterable[DatasetRef],
    destination: ResourcePathExpression,
    retrieval_callback: Callable[
        [Iterable[DatasetRef], ResourcePath, str, bool, bool],
        tuple[list[ResourcePath], dict[ResourcePath, list[DatasetId]], dict[ResourcePath, StoredFileInfo]],
    ],
) -> ResourcePath:
    """Retrieve artifacts from a Butler and place in ZIP file.

    Parameters
    ----------
    refs : `collections.abc.Iterable` [ `DatasetRef` ]
        The datasets to be included in the zip file. Must all be from
        the same dataset type.
    destination : `lsst.resources.ResourcePath`
        Directory to write the new ZIP file. This directory will
        also be used as a staging area for the datasets being downloaded
        from the datastore.
    retrieval_callback : `~collections.abc.Callable`
        Bound method for a function that can retrieve the artifacts and
        return the metadata necessary for creating the zip index. For example
        `lsst.daf.butler.datastore.Datastore.retrieveArtifacts`.

    Returns
    -------
    zip_file : `lsst.resources.ResourcePath`
        The path to the new ZIP file.
    """
    outdir = ResourcePath(destination, forceDirectory=True)
    if not outdir.isdir():
        raise ValueError(f"Destination location must refer to a directory. Given {destination}")
    # Simplest approach:
    # - create temp dir in destination
    # - Run retrieveArtifacts to that temp dir
    # - Create zip file with unique name.
    # - Delete temp dir
    # - Add index file to ZIP.
    # - Return name of zip file.
    with tempfile.TemporaryDirectory(dir=outdir.ospath, ignore_cleanup_errors=True) as tmpdir:
        tmpdir_path = ResourcePath(tmpdir, forceDirectory=True)
        paths, refmap, infomap = retrieval_callback(refs, tmpdir_path, "copy", True, False)
        # Will store the relative paths in the zip file.
        # These relative paths cannot be None but mypy doesn't know this.
        file_to_relative: dict[ResourcePath, str] = {}
        for p in paths:
            rel = p.relative_to(tmpdir_path)
            assert rel is not None
            file_to_relative[p] = rel

        # Name of zip file. Options are:
        # - uuid that changes every time.
        # - uuid5 based on file paths in zip
        # - uuid5 based on ref uuids.
        # - checksum derived from the above.
        # Start with uuid5 from file paths.
        data = ",".join(file_to_relative.values())
        # No need to come up with a different namespace.
        zip_uuid = uuid.uuid5(DatasetIdFactory.NS_UUID, data)
        zip_file_name = f"{zip_uuid}.zip"

        # Index maps relative path to DatasetRef.
        # The index has to:
        # - list all the DatasetRef
        # - map each artifact's path to a ref
        # - include the datastore records to allow formatter to be
        #   extracted for that path and component be associated.
        # Simplest not to try to include the full set of StoredItemInfo.

        # Convert the mappings to simplified form for pydantic.
        # and use the relative paths that will match the zip.
        simplified_refs = [ref.to_simple() for ref in refs]
        simplified_ref_map = {}
        for path, ids in refmap.items():
            simplified_ref_map[file_to_relative[path]] = ids
        simplified_info_map = {file_to_relative[path]: info.to_simple() for path, info in infomap.items()}

        index = ZipIndex(refs=simplified_refs, ref_map=simplified_ref_map, info_map=simplified_info_map)

        zip_path = outdir.join(zip_file_name, forceDirectory=False)
        with zipfile.ZipFile(zip_path.ospath, "w") as zip:
            zip.writestr("_index.json", index.model_dump_json(exclude_defaults=True, exclude_unset=True))
            for path, name in file_to_relative.items():
                zip.write(path.ospath, name)

    return zip_path
