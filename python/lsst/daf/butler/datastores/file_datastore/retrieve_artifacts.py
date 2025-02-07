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

__all__ = ("ZipIndex", "determine_destination_for_retrieved_artifact", "retrieve_and_zip", "unpack_zips")

import logging
import tempfile
import uuid
import zipfile
from collections.abc import Iterable
from typing import ClassVar, Literal, Protocol, Self

from pydantic import BaseModel

from lsst.daf.butler import (
    DatasetIdFactory,
    DatasetRef,
    SerializedDatasetRefContainers,
    SerializedDatasetRefContainerV1,
)
from lsst.daf.butler.datastore.stored_file_info import SerializedStoredFileInfo
from lsst.resources import ResourcePath, ResourcePathExpression

_LOG = logging.getLogger(__name__)


class ArtifactIndexInfo(BaseModel):
    """Information related to an artifact in an index."""

    info: SerializedStoredFileInfo
    """Datastore record information for this file artifact."""

    ids: set[uuid.UUID]
    """Dataset IDs for this artifact."""

    def append(self, id_: uuid.UUID) -> None:
        """Add an additional dataset ID.

        Parameters
        ----------
        id_ : `uuid.UUID`
            Additional dataset ID to associate with this artifact.
        """
        self.ids.add(id_)

    @classmethod
    def from_single(cls, info: SerializedStoredFileInfo, id_: uuid.UUID) -> Self:
        """Create a mapping from a single ID and info.

        Parameters
        ----------
        info : `SerializedStoredFileInfo`
            Datastore record for this artifact.
        id_ : `uuid.UUID`
            First dataset ID to associate with this artifact.
        """
        return cls(info=info, ids=[id_])

    def subset(self, ids: Iterable[uuid.UUID]) -> Self:
        """Replace the IDs with a subset of the IDs and return a new instance.

        Parameters
        ----------
        ids : `~collections.abc.Iterable` [ `uuid.UUID` ]
            Subset of IDs to keep.

        Returns
        -------
        subsetted : `ArtifactIndexInfo`
            New instance with the requested subset.

        Raises
        ------
        ValueError
            Raised if the given IDs is not a subset of the current IDs.
        """
        subset = set(ids)
        if subset - self.ids:
            raise ValueError(f"Given subset of {subset} is not a subset of {self.ids}")
        return type(self)(ids=subset, info=self.info.model_copy())


class ZipIndex(BaseModel):
    """Index of a Zip file of Butler datasets.

    A file can be associated with multiple butler datasets and a single
    butler dataset can be associated with multiple files. This model
    provides the necessary information for ingesting back into a Butler
    file datastore.
    """

    index_version: Literal["V1"] = "V1"

    refs: SerializedDatasetRefContainers
    """Deduplicated information for all the `DatasetRef` in the index."""

    artifact_map: dict[str, ArtifactIndexInfo]
    """Mapping of each Zip member to associated lookup information."""

    index_name: ClassVar[str] = "_butler_zip_index.json"
    """Name to use when saving the index to a file."""

    def generate_uuid5(self) -> uuid.UUID:
        """Create a UUID based on the Zip index.

        Returns
        -------
        id_ : `uuid.UUID`
            A UUID5 created from the paths inside the Zip file. Guarantees
            that if the Zip file is regenerated with exactly the same file
            paths the same answer will be returned.
        """
        # Options are:
        # - uuid5 based on file paths in zip
        # - uuid5 based on ref uuids.
        # - checksum derived from the above.
        # - uuid5 from file paths and dataset refs.
        # Do not attempt to include file contents in UUID.
        # Start with uuid5 from file paths.
        data = ",".join(sorted(self.artifact_map.keys()))
        # No need to come up with a different namespace.
        return uuid.uuid5(DatasetIdFactory.NS_UUID, data)

    def __len__(self) -> int:
        """Return the number of files in the Zip."""
        return len(self.artifact_map)

    def calculate_zip_file_name(self) -> str:
        """Calculate the default name for the Zip file based on the index
        contents.

        Returns
        -------
        name : `str`
            Name of the zip file based on index.
        """
        return f"{self.generate_uuid5()}.zip"

    def calculate_zip_file_path_in_store(self) -> str:
        """Calculate the relative path inside a datastore that should be
        used for this Zip file.

        Returns
        -------
        path_in_store : `str`
            Relative path to use for Zip file in datastore.
        """
        zip_name = self.calculate_zip_file_name()
        return f"zips/{zip_name[:4]}/{zip_name}"

    def write_index(self, dir: ResourcePath) -> ResourcePath:
        """Write the index to the specified directory.

        Parameters
        ----------
        dir : `~lsst.resources.ResourcePath`
            Directory to write the index file to.

        Returns
        -------
        index_path : `~lsst.resources.ResourcePath`
            Path to the index file that was written.
        """
        index_path = dir.join(self.index_name, forceDirectory=False)
        with index_path.open("w") as fd:
            # Need to include unset/default values so that the version
            # discriminator field for refs container appears in the
            # serialization.
            print(self.model_dump_json(), file=fd)
        return index_path

    @classmethod
    def calc_relative_paths(
        cls, root: ResourcePath, paths: Iterable[ResourcePath]
    ) -> dict[ResourcePath, str]:
        """Calculate the path to use inside the Zip file from the full path.

        Parameters
        ----------
        root : `lsst.resources.ResourcePath`
            The reference root directory.
        paths : `~collections.abc.Iterable` [ `lsst.resources.ResourcePath` ]
            The paths to the files that should be included in the Zip file.

        Returns
        -------
        abs_to_rel : `dict` [ `~lsst.resources.ResourcePath`, `str` ]
            Mapping of the original file path to the relative path to use
            in Zip file.
        """
        file_to_relative: dict[ResourcePath, str] = {}
        for p in paths:
            # It is an error if there is no relative path.
            rel = p.relative_to(root)
            if rel is None:
                raise RuntimeError(f"Unexepectedly unable to calculate relative path of {p} to {root}.")
            file_to_relative[p] = rel
        return file_to_relative

    @classmethod
    def from_artifact_map(
        cls,
        refs: Iterable[DatasetRef],
        artifact_map: dict[ResourcePath, ArtifactIndexInfo],
        root: ResourcePath,
    ) -> Self:
        """Create an index from the mappings returned from
        `Datastore.retrieveArtifacts`.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets present in the index.
        artifact_map : `dict` [ `lsst.resources.ResourcePath`, `ArtifactMap` ]
            Mapping of artifact path to information linking it to the
            associated refs and datastore information.
        root : `lsst.resources.ResourcePath`
            Root path to be removed from all the paths before creating the
            index.
        """
        if not refs:
            return cls(
                refs=SerializedDatasetRefContainerV1.from_refs(refs),
                artifact_map={},
            )

        # Calculate the paths relative to the given root since the Zip file
        # uses relative paths.
        file_to_relative = cls.calc_relative_paths(root, artifact_map.keys())

        simplified_refs = SerializedDatasetRefContainerV1.from_refs(refs)

        # Convert the artifact mapping to relative path.
        relative_artifact_map = {file_to_relative[path]: info for path, info in artifact_map.items()}

        return cls(
            refs=simplified_refs,
            artifact_map=relative_artifact_map,
        )

    @classmethod
    def from_zip_file(cls, zip_path: ResourcePath) -> Self:
        """Given a path to a Zip file return the index.

        Parameters
        ----------
        zip_path : `lsst.resources.ResourcePath`
            Path to the Zip file.
        """
        with zip_path.open("rb") as fd, zipfile.ZipFile(fd) as zf:
            json_data = zf.read(cls.index_name)
        return cls.model_validate_json(json_data)


def determine_destination_for_retrieved_artifact(
    destination_directory: ResourcePath, source_path: ResourcePath, preserve_path: bool, prefix: str = ""
) -> ResourcePath:
    """Determine destination path for an artifact retrieved from a datastore.

    Parameters
    ----------
    destination_directory : `ResourcePath`
        Path to the output directory where file will be stored.
    source_path : `ResourcePath`
        Path to the source file to be transferred.  This may be relative to the
        datastore root, or an absolute path.
    preserve_path : `bool`
        If `True` the full path of the artifact within the datastore
        is preserved. If `False` the final file component of the path
        is used.
    prefix : `str`, optional
        Prefix to add to the file name if ``preserve_path`` is `False`.

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
        if prefix:
            target_path = prefix + target_path

    target_uri = destination_directory.join(target_path).abspath()
    if target_uri.relative_to(destination_directory) is None:
        raise ValueError(f"File path attempts to escape destination directory: '{source_path}'")
    return target_uri


class RetrievalCallable(Protocol):
    def __call__(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePath,
        transfer: str,
        preserve_path: bool,
        overwrite: bool,
        write_index: bool,
        add_prefix: bool,
    ) -> dict[ResourcePath, ArtifactIndexInfo]: ...


def retrieve_and_zip(
    refs: Iterable[DatasetRef],
    destination: ResourcePathExpression,
    retrieval_callback: RetrievalCallable,
    overwrite: bool = True,
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
    overwrite : `bool`, optional
        If `False` the output Zip will not be written if a file of the
        same name is already present in ``destination``.

    Returns
    -------
    zip_file : `lsst.resources.ResourcePath`
        The path to the new ZIP file.

    Raises
    ------
    ValueError
        Raised if there are no refs to retrieve.
    """
    if not refs:
        raise ValueError("Requested Zip file with no contents.")

    outdir = ResourcePath(destination, forceDirectory=True)
    if not outdir.isdir():
        raise ValueError(f"Destination location must refer to a directory. Given {destination}")

    if not outdir.exists():
        outdir.mkdir()

    # Simplest approach:
    # - create temp dir in destination
    # - Run retrieveArtifacts to that temp dir
    # - Create zip file with unique name.
    # - Delete temp dir
    # - Add index file to ZIP.
    # - Return name of zip file.
    with tempfile.TemporaryDirectory(dir=outdir.ospath, ignore_cleanup_errors=True) as tmpdir:
        tmpdir_path = ResourcePath(tmpdir, forceDirectory=True)
        # Retrieve the artifacts and write the index file. Strip paths.
        artifact_map = retrieval_callback(
            refs=refs,
            destination=tmpdir_path,
            transfer="auto",
            preserve_path=False,
            overwrite=False,
            write_index=True,
            add_prefix=True,
        )

        # Read the index to construct file name.
        index_path = tmpdir_path.join(ZipIndex.index_name, forceDirectory=False)
        index_json = index_path.read()
        index = ZipIndex.model_validate_json(index_json)

        # Use unique name based on files in Zip.
        zip_file_name = index.calculate_zip_file_name()
        zip_path = outdir.join(zip_file_name, forceDirectory=False)
        if not overwrite and zip_path.exists():
            raise FileExistsError(f"Output Zip at {zip_path} already exists but cannot overwrite.")
        with zipfile.ZipFile(zip_path.ospath, "w") as zip:
            zip.write(index_path.ospath, index_path.basename(), compress_type=zipfile.ZIP_DEFLATED)
            for path, name in index.calc_relative_paths(tmpdir_path, list(artifact_map)).items():
                zip.write(path.ospath, name)

    return zip_path


def unpack_zips(
    zips_to_transfer: Iterable[ResourcePath],
    allowed_ids: set[uuid.UUID],
    destination: ResourcePath,
    preserve_path: bool,
    overwrite: bool,
) -> dict[ResourcePath, ArtifactIndexInfo]:
    """Transfer the Zip files and unpack them in the destination directory.

    Parameters
    ----------
    zips_to_transfer : `~collections.abc.Iterable` \
            [ `~lsst.resources.ResourcePath` ]
        Paths to Zip files to unpack. These must be Zip files that include
        the index information and were created by the Butler.
    allowed_ids : `set` [ `uuid.UUID` ]
        All the possible dataset IDs for which artifacts should be extracted
        from the Zip file. If an ID in the Zip file is not present in this
        list the artifact will not be extracted from the Zip.
    destination : `~lsst.resources.ResourcePath`
        Output destination for the Zip contents.
    preserve_path : `bool`
        Whether to include subdirectories during extraction. If `True` a
        directory will be made per Zip.
    overwrite : `bool`, optional
        If `True` allow transfers to overwrite existing files at the
        destination.

    Returns
    -------
    artifact_map : `dict` \
            [ `~lsst.resources.ResourcePath`, `ArtifactIndexInfo` ]
        Path linking Zip contents location to associated artifact information.
    """
    artifact_map: dict[ResourcePath, ArtifactIndexInfo] = {}
    for source_uri in zips_to_transfer:
        _LOG.debug("Unpacking zip file %s", source_uri)
        # Assume that downloading to temporary location is more efficient
        # than trying to read the contents remotely.
        with ResourcePath.temporary_uri(suffix=".zip") as temp:
            temp.transfer_from(source_uri, transfer="auto")
            index = ZipIndex.from_zip_file(temp)

            if preserve_path:
                subdir = ResourcePath(
                    index.calculate_zip_file_path_in_store(), forceDirectory=False, forceAbsolute=False
                ).dirname()
                outdir = destination.join(subdir)
            else:
                outdir = destination
            outdir.mkdir()
            with temp.open("rb") as fd, zipfile.ZipFile(fd) as zf:
                for path_in_zip, artifact_info in index.artifact_map.items():
                    # Skip if this specific dataset ref is not requested.
                    included_ids = artifact_info.ids & allowed_ids
                    if included_ids:
                        # Do not apply a new prefix since the zip file
                        # should already have a prefix.
                        output_path = outdir.join(path_in_zip, forceDirectory=False)
                        if not overwrite and output_path.exists():
                            raise FileExistsError(
                                f"Destination path '{output_path}' already exists. "
                                "Extraction from Zip cannot be completed."
                            )
                        zf.extract(path_in_zip, path=outdir.ospath)
                        artifact_map[output_path] = artifact_info.subset(included_ids)
    return artifact_map
