import unittest

from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available
from lsst.daf.butler.tests.utils import mock_env

if butler_server_is_available:
    from lsst.daf.butler.remote_butler.authentication import cadc
    from lsst.daf.butler.remote_butler.authentication.rubin import (
        _EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY,
        _RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY,
        RubinAuthenticationProvider,
        _get_authentication_token_from_environment,
    )


@unittest.skipIf(not butler_server_is_available, butler_server_import_error)
class TestButlerClientAuthentication(unittest.TestCase):
    """Test access-token logic"""

    def test_explicit_butler_token(self):
        with mock_env(
            {
                _EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token1",
                _RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "not-this-token",
            }
        ):
            token = _get_authentication_token_from_environment("https://untrustedserver.com")
            self.assertEqual(token, "token1")

    def test_jupyter_token_with_safe_server(self):
        with mock_env({_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token2"}):
            token = _get_authentication_token_from_environment("https://data.LSST.cloud/butler")
            self.assertEqual(token, "token2")

    def test_jupyter_token_with_unsafe_server(self):
        with mock_env({_RSP_JUPYTER_ACCESS_TOKEN_ENVIRONMENT_KEY: "token2"}):
            token = _get_authentication_token_from_environment("https://untrustedserver.com/butler")
            self.assertIsNone(token)

    def test_missing_token(self):
        with mock_env({}):
            token = _get_authentication_token_from_environment("https://data.lsst.cloud/butler")
            self.assertIsNone(token)

    def test_header_generation(self):
        auth = RubinAuthenticationProvider("tokendata")
        self.assertEqual(auth.get_server_headers(), {"Authorization": "Bearer tokendata"})
        # At the Rubin Science Platform, the server sends pre-signed URLs that
        # do not require authentication.
        self.assertEqual(auth.get_datastore_headers(), {})

    def test_cadc_auth(self):
        auth = cadc.CadcAuthenticationProvider("tokendata")
        self.assertEqual(auth.get_server_headers(), {})
        self.assertEqual(auth.get_datastore_headers(), {"Authorization": "Bearer tokendata"})
        with mock_env({cadc._CADC_TOKEN_ENVIRONMENT_KEY: "tokendata"}):
            token = cadc._get_authentication_token_from_environment("https://www.canfar.net/butler")
            self.assertEqual(token, "tokendata")
