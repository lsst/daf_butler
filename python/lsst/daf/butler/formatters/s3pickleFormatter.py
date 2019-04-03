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

"""Formatter associated with Python pickled objects."""

__all__ = ("S3PickleFormatter", )

import os
import pickle
import boto3

from lsst.daf.butler.formatters.fileFormatter import FileFormatter


class S3PickleFormatter(FileFormatter):
    """Interface for reading and writing Python objects to and from pickle
    files.
    """
    extension = ".pickle"

    unsupportedParameters = None
    """This formatter does not support any parameters"""

    def _readFile(self, path, pytype=None):
        """Read a file from the path in pickle format.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `class`, optional
            Not used by this implementation.

        Returns
        -------
        data : `object`
            Either data as Python object read from the pickle file, or None
            if the file could not be opened.
        """
        try:
            tmpdir = "/home/dinob/uni/lsstspark/simple_repo/s3_repo/"
            relpath, name = os.path.split(path)
            tmppath = os.path.join(tmpdir, name)
            s3client = boto3.client('s3')
            # why does the pickler not use fileDescriptor....
            # why would readFile not find anything
            s3client.download_file('lsstspark', path, tmppath)
            with open(tmppath, "rb") as fd:
                data = pickle.load(fd)
        except FileNotFoundError:
            data = None

        return data

    def _writeFile(self, inMemoryDataset, fileDescriptor):
        """Write the in memory dataset to file on disk.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.
        fileDescriptor : `FileDescriptor`
            Details of the file to be written.

        Raises
        ------
        Exception
            The file could not be written.
        """
        tmpdir = "/home/dinob/uni/lsstspark/simple_repo/s3_repo/"
        locpath, name = os.path.split(fileDescriptor.location.path)
        tmppath = os.path.join(tmpdir, name)

        with open(tmppath, "wb") as fd:
            pickle.dump(inMemoryDataset, fd, protocol=-1)

        s3client = boto3.client('s3')
        s3client.upload_file(tmppath, fileDescriptor.location._bucket, fileDescriptor.location.path)

