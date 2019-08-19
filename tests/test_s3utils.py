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
import unittest

try:
    import boto3
    import botocore
    from moto import mock_s3
except ImportError:
    boto3 = None

    def mock_s3(cls):
        """A no-op decorator in case moto mock_s3 can not be imported.
        """
        return cls

from lsst.daf.butler.core.s3utils import bucketExists, s3CheckFileExists
from lsst.daf.butler.core.location import Location, ButlerURI


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
@mock_s3
class S3UtilsTestCase(unittest.TestCase):
    """Test for the S3 related utilities.
    """
    bucketName = "testBucketName"
    fileName = "testFileName"

    def setUp(self):
        # set up some fake credentials if they do not exist
        if not os.path.exists("~/.aws/credentials"):
            if "AWS_ACCESS_KEY_ID" not in os.environ:
                os.environ["AWS_ACCESS_KEY_ID"] = "dummyAccessKeyId"
            if "AWS_SECRET_ACCESS_KEY" not in os.environ:
                os.environ["AWS_SECRET_ACCESS_KEY"] = "dummySecreyAccessKey"

        s3 = boto3.client("s3")
        try:
            s3.create_bucket(Bucket=self.bucketName)
            s3.put_object(Bucket=self.bucketName, Key=self.fileName,
                          Body=b"test content")
        except s3.exceptions.BucketAlreadyExists:
            pass

    def tearDown(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as err:
            errorcode = err.response["ResponseMetadata"]["HTTPStatusCode"]
            if errorcode == 404:
                # the key does not exists - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # unset any potentially set dummy credentials
        keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        for key in keys:
            if key in os.environ and "dummy" in os.environ[key]:
                del os.environ[key]

    def testBucketExists(self):
        self.assertTrue(bucketExists(f"{self.bucketName}"))
        self.assertFalse(bucketExists(f"{self.bucketName}_NO_EXIST"))

    def testFileExists(self):
        s3 = boto3.client('s3')
        self.assertTrue(s3CheckFileExists(client=s3, bucket=self.bucketName,
                                          path=self.fileName)[0])
        self.assertFalse(s3CheckFileExists(client=s3, bucket=self.bucketName,
                                           path=self.fileName+"_NO_EXIST")[0])

        datastoreRootUri = f"s3://{self.bucketName}/"
        uri = f"s3://{self.bucketName}/{self.fileName}"

        buri = ButlerURI(uri)
        location = Location(datastoreRootUri, self.fileName)

        self.assertTrue(s3CheckFileExists(client=s3, path=buri)[0])
        # just to make sure the overloaded keyword works correctly
        self.assertTrue(s3CheckFileExists(buri, client=s3)[0])
        self.assertTrue(s3CheckFileExists(client=s3, path=location)[0])

        # make sure supplying strings resolves correctly too
        self.assertTrue(s3CheckFileExists(uri, client=s3))
        self.assertTrue(s3CheckFileExists(uri))


if __name__ == "__main__":
    unittest.main()
