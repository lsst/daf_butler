# This file is part of daf_butler
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

from lsst.daf.butler.registry import DbAuth, DbAuthError

TESTDIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config", "dbAuth")


class DbAuthTestCase(unittest.TestCase):
    """Test DbAuth class."""

    def test_patterns(self):
        """Test various patterns against a fixed connection URL."""
        for matchPattern in [
            "postgresql://*",
            "postgresql://*.example.com",
            "postgresql://*.example.com/my_*",
            "postgresql://host.example.com/my_database",
            "postgresql://host.example.com:5432/my_database",
            "postgresql://user@host.example.com/my_database",
        ]:
            auth = DbAuth(authList=[dict(url=matchPattern, username="foo", password="bar")])
            user, pwd = auth.getAuth("postgresql", "user", "host.example.com", "5432", "my_database")
            self.assertEqual(user, "user")
            self.assertEqual(pwd, "bar")

    def test_connStrings(self):
        """Test various connection URLs against a fixed pattern."""
        auth = DbAuth(
            authList=[dict(url="postgresql://host.example.com/my_database", username="foo", password="bar")]
        )
        for connComponents in [
            ("postgresql", "user", "host.example.com", 5432, "my_database"),
            ("postgresql", "user", "host.example.com", None, "my_database"),
        ]:
            user, pwd = auth.getAuth(*connComponents)
            self.assertEqual(user, "user")
            self.assertEqual(pwd, "bar")
        for connComponents in [
            ("postgresql", None, "host.example.com", None, "my_database"),
            ("postgresql", None, "host.example.com", "5432", "my_database"),
        ]:
            user, pwd = auth.getAuth(*connComponents)
            self.assertEqual(user, "foo")
            self.assertEqual(pwd, "bar")

    def test_load(self):
        """Test loading from a YAML file and matching in order."""
        filePath = os.path.join(TESTDIR, "db-auth.yaml")
        os.chmod(filePath, 0o600)
        auth = DbAuth(filePath)
        self.assertEqual(
            auth.getAuth("postgresql", "user", "host.example.com", "5432", "my_database"), ("user", "test1")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user", "host.example.com", "3360", "my_database"), ("user", "test2")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "", "host.example.com", "5432", "my_database"), ("user", "test3")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host.example.com", "", "my_database"), ("user", "test4")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "", "host2.example.com", None, "my_other"), ("user", "test5")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host3.example.com", 3360, "some_database"), ("user", "test6")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user", "host4.other.com", None, "other_database"), ("user", "test8")
        )

    def test_ipv6(self):
        """Test IPv6 addresses as host names."""
        auth = DbAuth(
            authList=[
                dict(url="postgresql://user@[fe80::1ff:fe23:4567:890a]:5432/db", password="pwd"),
                dict(url="postgresql://[fe80::1ff:fe23:4567:890a]:5432/db", password="pwd2"),
                dict(url="postgresql://user3@[fe80::1ff:fe23:4567:890a]/db", password="pwd3"),
                dict(url="postgresql://[fe80::1ff:fe23:4567:890a]/db", password="pwd4"),
            ]
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user", "fe80::1ff:fe23:4567:890a", "5432", "db"), ("user", "pwd")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user2", "fe80::1ff:fe23:4567:890a", "5432", "db"), ("user2", "pwd2")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user3", "fe80::1ff:fe23:4567:890a", "3360", "db"), ("user3", "pwd3")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user4", "fe80::1ff:fe23:4567:890a", "3360", "db"), ("user4", "pwd4")
        )

    def test_search(self):
        """Test ordered searching."""
        auth = DbAuth(
            authList=[
                dict(url="postgresql://user1@host2.example.com:3360/database3", password="first"),
                dict(
                    url="postgresql://host2.example.com:3360/database3",
                    username="second_u",
                    password="second",
                ),
                dict(url="postgresql://host2.example.com:5432/database3", username="wrong", password="port"),
                dict(url="postgresql://host3.example.com/", username="third_u", password="third"),
                dict(url="oracle://oracle.example.com/other_database", username="scott", password="tiger"),
                dict(url="postgresql://*.example.com/database3", username="fourth_u", password="fourth"),
                dict(url="postgresql://*.example.com", username="fifth_u", password="fifth"),
            ]
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user1", "host2.example.com", "3360", "database3"), ("user1", "first")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "user2", "host2.example.com", "3360", "database3"), ("user2", "second")
        )
        self.assertEqual(
            auth.getAuth("postgresql", "", "host2.example.com", "3360", "database3"), ("second_u", "second")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host2.example.com", "3360", "database3"), ("second_u", "second")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host3.example.com", "3360", "database3"), ("third_u", "third")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host3.example.com", "3360", "database4"), ("third_u", "third")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host4.example.com", "3360", "database3"), ("fourth_u", "fourth")
        )
        self.assertEqual(
            auth.getAuth("postgresql", None, "host4.example.com", "3360", "database4"), ("fifth_u", "fifth")
        )

    def test_errors(self):
        """Test for error exceptions."""
        with self.assertRaisesRegex(DbAuthError, r"^No default path provided to DbAuth configuration file$"):
            auth = DbAuth()
        with self.assertRaisesRegex(DbAuthError, r"^No DbAuth configuration file: .*this_does_not_exist$"):
            auth = DbAuth(os.path.join(TESTDIR, "this_does_not_exist"))
        with self.assertRaisesRegex(DbAuthError, r"^No DbAuth configuration file: .*this_does_not_exist$"):
            auth = DbAuth(os.path.join(TESTDIR, "this_does_not_exist"), envVar="LSST_NONEXISTENT")
        with self.assertRaisesRegex(
            DbAuthError,
            r"^DbAuth configuration file .*badDbAuth1.yaml has incorrect " r"permissions: \d*644$",
        ):
            filePath = os.path.join(TESTDIR, "badDbAuth1.yaml")
            os.chmod(filePath, 0o644)
            auth = DbAuth(filePath)
        with self.assertRaisesRegex(
            DbAuthError, r"^Unable to load DbAuth configuration file: " r".*badDbAuth2.yaml"
        ):
            filePath = os.path.join(TESTDIR, "badDbAuth2.yaml")
            os.chmod(filePath, 0o600)
            os.environ["LSST_DB_AUTH_TEST"] = filePath
            auth = DbAuth(envVar="LSST_DB_AUTH_TEST")

        auth = DbAuth(authList=[])
        with self.assertRaisesRegex(DbAuthError, r"^Missing dialectname parameter$"):
            auth.getAuth(None, None, None, None, None)
        with self.assertRaisesRegex(DbAuthError, r"^Missing dialectname parameter$"):
            auth.getAuth("", None, None, None, None)
        with self.assertRaisesRegex(DbAuthError, r"^Missing host parameter$"):
            auth.getAuth("postgresql", None, None, None, None)
        with self.assertRaisesRegex(DbAuthError, r"^Missing host parameter$"):
            auth.getAuth("postgresql", None, "", None, None)
        with self.assertRaisesRegex(DbAuthError, r"^Missing database parameter$"):
            auth.getAuth("postgresql", None, "example.com", None, None)
        with self.assertRaisesRegex(DbAuthError, r"^Missing database parameter$"):
            auth.getAuth("postgresql", None, "example.com", None, "")
        with self.assertRaisesRegex(
            DbAuthError,
            r"^No matching DbAuth configuration for: " r"\(postgresql, None, example\.com, None, foo\)$",
        ):
            auth.getAuth("postgresql", None, "example.com", None, "foo")

        auth = DbAuth(authList=[dict(password="testing")])
        with self.assertRaisesRegex(DbAuthError, r"^Missing URL in DbAuth configuration$"):
            auth.getAuth("postgresql", None, "example.com", None, "foo")

        auth = DbAuth(authList=[dict(url="testing", password="testing")])
        with self.assertRaisesRegex(DbAuthError, r"^Missing database dialect in URL: testing$"):
            auth.getAuth("postgresql", None, "example.com", None, "foo")

        auth = DbAuth(authList=[dict(url="postgresql:///foo", password="testing")])
        with self.assertRaisesRegex(DbAuthError, r"^Missing host in URL: postgresql:///foo$"):
            auth.getAuth("postgresql", None, "example.com", None, "foo")

    def test_getUrl(self):
        """Repeat relevant tests using getUrl interface."""
        for matchPattern in [
            "postgresql://*",
            "postgresql://*.example.com",
            "postgresql://*.example.com/my_*",
            "postgresql://host.example.com/my_database",
            "postgresql://host.example.com:5432/my_database",
            "postgresql://user@host.example.com/my_database",
        ]:
            auth = DbAuth(authList=[dict(url=matchPattern, username="foo", password="bar")])
            self.assertEqual(
                auth.getUrl("postgresql://user@host.example.com:5432/my_database"),
                "postgresql://user:bar@host.example.com:5432/my_database",
            )

        auth = DbAuth(
            authList=[dict(url="postgresql://host.example.com/my_database", username="foo", password="bar")]
        )
        self.assertEqual(
            auth.getUrl("postgresql://user@host.example.com:5432/my_database"),
            "postgresql://user:bar@host.example.com:5432/my_database",
        ),
        self.assertEqual(
            auth.getUrl("postgresql://user@host.example.com/my_database"),
            "postgresql://user:bar@host.example.com/my_database",
        ),
        self.assertEqual(
            auth.getUrl("postgresql://host.example.com/my_database"),
            "postgresql://foo:bar@host.example.com/my_database",
        ),
        self.assertEqual(
            auth.getUrl("postgresql://host.example.com:5432/my_database"),
            "postgresql://foo:bar@host.example.com:5432/my_database",
        ),

        filePath = os.path.join(TESTDIR, "db-auth.yaml")
        os.chmod(filePath, 0o600)
        auth = DbAuth(filePath)
        self.assertEqual(
            auth.getUrl("postgresql://user@host.example.com:5432/my_database"),
            "postgresql://user:test1@host.example.com:5432/my_database",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user@host.example.com:3360/my_database"),
            "postgresql://user:test2@host.example.com:3360/my_database",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host.example.com:5432/my_database"),
            "postgresql://user:test3@host.example.com:5432/my_database",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host.example.com/my_database"),
            "postgresql://user:test4@host.example.com/my_database",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host2.example.com/my_other"),
            "postgresql://user:test5@host2.example.com/my_other",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host3.example.com:3360/some_database"),
            "postgresql://user:test6@host3.example.com:3360/some_database",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user@host4.other.com/other_database"),
            "postgresql://user:test8@host4.other.com/other_database",
        )

        auth = DbAuth(
            authList=[
                dict(url="postgresql://user@[fe80::1ff:fe23:4567:890a]:5432/db", password="pwd"),
                dict(url="postgresql://[fe80::1ff:fe23:4567:890a]:5432/db", password="pwd2"),
                dict(url="postgresql://user3@[fe80::1ff:fe23:4567:890a]/db", password="pwd3"),
                dict(url="postgresql://[fe80::1ff:fe23:4567:890a]/db", password="pwd4"),
            ]
        )
        self.assertEqual(
            auth.getUrl("postgresql://user@[fe80::1ff:fe23:4567:890a]:5432/db"),
            "postgresql://user:pwd@[fe80::1ff:fe23:4567:890a]:5432/db",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user2@[fe80::1ff:fe23:4567:890a]:5432/db"),
            "postgresql://user2:pwd2@[fe80::1ff:fe23:4567:890a]:5432/db",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user3@[fe80::1ff:fe23:4567:890a]:3360/db"),
            "postgresql://user3:pwd3@[fe80::1ff:fe23:4567:890a]:3360/db",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user4@[fe80::1ff:fe23:4567:890a]:3360/db"),
            "postgresql://user4:pwd4@[fe80::1ff:fe23:4567:890a]:3360/db",
        )

        auth = DbAuth(
            authList=[
                dict(url="postgresql://user1@host2.example.com:3360/database3", password="first"),
                dict(
                    url="postgresql://host2.example.com:3360/database3",
                    username="second_u",
                    password="second",
                ),
                dict(url="postgresql://host2.example.com:5432/database3", username="wrong", password="port"),
                dict(url="postgresql://host3.example.com/", username="third_u", password="third"),
                dict(url="oracle://oracle.example.com/other_database", username="scott", password="tiger"),
                dict(url="postgresql://*.example.com/database3", username="fourth_u", password="fourth"),
                dict(url="postgresql://*.example.com", username="fifth_u", password="fifth"),
            ]
        )
        self.assertEqual(
            auth.getUrl("postgresql://user1@host2.example.com:3360/database3"),
            "postgresql://user1:first@host2.example.com:3360/database3",
        )
        self.assertEqual(
            auth.getUrl("postgresql://user2@host2.example.com:3360/database3"),
            "postgresql://user2:second@host2.example.com:3360/database3",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host2.example.com:3360/database3"),
            "postgresql://second_u:second@host2.example.com:3360/database3",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host3.example.com:3360/database3"),
            "postgresql://third_u:third@host3.example.com:3360/database3",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host3.example.com:3360/database4"),
            "postgresql://third_u:third@host3.example.com:3360/database4",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host4.example.com:3360/database3"),
            "postgresql://fourth_u:fourth@host4.example.com:3360/database3",
        )
        self.assertEqual(
            auth.getUrl("postgresql://host4.example.com:3360/database4"),
            "postgresql://fifth_u:fifth@host4.example.com:3360/database4",
        )

        auth = DbAuth(authList=[])
        with self.assertRaisesRegex(DbAuthError, r"^Missing dialectname parameter$"):
            auth.getUrl("/")
        with self.assertRaisesRegex(DbAuthError, r"^Missing host parameter$"):
            auth.getUrl("postgresql:///")
        with self.assertRaisesRegex(DbAuthError, r"^Missing database parameter$"):
            auth.getUrl("postgresql://example.com")
        with self.assertRaisesRegex(DbAuthError, r"^Missing database parameter$"):
            auth.getUrl("postgresql://example.com/")
        with self.assertRaisesRegex(
            DbAuthError,
            r"^No matching DbAuth configuration for: " r"\(postgresql, None, example\.com, None, foo\)$",
        ):
            auth.getUrl("postgresql://example.com/foo")

    def test_urlEncoding(self):
        """Test URL encoding of username and password."""
        auth = DbAuth(
            authList=[
                dict(
                    url="postgresql://host.example.com/my_database",
                    username="foo:étude",
                    password="bar,.@%&/*[]",
                )
            ]
        )
        self.assertEqual(
            auth.getUrl("postgresql://host.example.com:5432/my_database"),
            "postgresql://foo%3A%C3%A9tude:bar%2C.%40%25%26%2F%2A%5B%5D@host.example.com:5432/my_database",
        )


if __name__ == "__main__":
    unittest.main()
