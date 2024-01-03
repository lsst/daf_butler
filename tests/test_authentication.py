import unittest

from lsst.daf.butler.tests.utils import mock_env

try:
    from lsst.daf.butler.remote_butler import RemoteButler
    from lsst.daf.butler.remote_butler._authentication import (
        _EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY,
        _RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY,
        get_authentication_headers,
        get_authentication_token_from_environment,
    )
except ImportError:
    RemoteButler = None


@unittest.skipIf(
    RemoteButler is None, "RemoteButler could not be imported, optional dependencies may not be installed"
)
class TestButlerClientAuthentication(unittest.TestCase):
    """Test access-token logic"""

    def test_explicit_butler_token(self):
        with mock_env(
            {
                _EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token1",
                _RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "not-this-token",
            }
        ):
            token = get_authentication_token_from_environment("https://untrustedserver.com")
            self.assertEqual(token, "token1")

    def test_jupyter_token_with_safe_server(self):
        with mock_env({_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token2"}):
            token = get_authentication_token_from_environment("https://data.LSST.cloud/butler")
            self.assertEqual(token, "token2")

    def test_jupyter_token_with_unsafe_server(self):
        with mock_env({_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token2"}):
            token = get_authentication_token_from_environment("https://untrustedserver.com/butler")
            self.assertIsNone(token)

    def test_missing_token(self):
        with mock_env({}):
            token = get_authentication_token_from_environment("https://data.lsst.cloud/butler")
            self.assertIsNone(token)

    def test_header_generation(self):
        headers = get_authentication_headers("tokendata")
        self.assertEqual(headers, {"Authorization": "Bearer tokendata"})
