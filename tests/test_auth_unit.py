"""
Unit tests for arrowjet.auth — credential resolution.

Tests the ResolvedCredentials container and Redshift-specific
auth flows (IAM, Secrets Manager, password) using mocked AWS calls.
No real AWS credentials or Redshift cluster needed.
"""

import json
from unittest.mock import MagicMock, patch
from urllib.parse import unquote, urlparse

import pytest

from arrowjet.auth.base import ResolvedCredentials
from arrowjet.auth.redshift import (
    extract_cluster_identifier,
    extract_workgroup_name,
    infer_region,
    is_serverless,
    resolve_credentials,
)


# ===================================================================
# ResolvedCredentials
# ===================================================================


class TestResolvedCredentials:
    def test_as_kwargs(self):
        creds = ResolvedCredentials(
            host="host.example.com", port=5439, database="dev",
            user="admin", password="secret",
        )
        kw = creds.as_kwargs()
        assert kw["host"] == "host.example.com"
        assert kw["port"] == 5439
        assert kw["database"] == "dev"
        assert kw["user"] == "admin"
        assert kw["password"] == "secret"
        assert kw["ssl"] is True

    def test_as_kwargs_no_ssl(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret", ssl=False,
        )
        kw = creds.as_kwargs()
        assert "ssl" not in kw

    def test_as_kwargs_with_extra(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret",
            extra={"timeout": 30},
        )
        kw = creds.as_kwargs()
        assert kw["timeout"] == 30

    def test_as_uri_basic(self):
        creds = ResolvedCredentials(
            host="host.example.com", port=5439, database="dev",
            user="admin", password="secret",
        )
        uri = creds.as_uri()
        parsed = urlparse(uri)
        assert parsed.scheme == "postgresql"
        assert parsed.hostname == "host.example.com"
        assert parsed.port == 5439
        assert parsed.username == "admin"
        assert unquote(parsed.password) == "secret"
        assert parsed.path == "/dev"

    def test_as_uri_special_chars_in_password(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="IAM:admin", password="p@ss/w0rd!&foo=bar",
        )
        uri = creds.as_uri()
        parsed = urlparse(uri)
        assert unquote(parsed.username) == "IAM:admin"
        assert unquote(parsed.password) == "p@ss/w0rd!&foo=bar"

    def test_as_uri_custom_scheme(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret",
        )
        uri = creds.as_uri(scheme="redshift+arrowjet")
        assert uri.startswith("redshift+arrowjet://")

    def test_as_uri_with_ssl(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret", ssl=True,
        )
        uri = creds.as_uri()
        assert "sslmode=require" in uri

    def test_as_uri_without_ssl(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret", ssl=False,
        )
        uri = creds.as_uri()
        assert "sslmode" not in uri

    def test_repr_hides_password(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret",
        )
        r = repr(creds)
        assert "secret" not in r
        assert "***" in r

    def test_repr_empty_password(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="",
        )
        r = repr(creds)
        assert "(empty)" in r

    def test_frozen(self):
        creds = ResolvedCredentials(
            host="host", port=5439, database="dev",
            user="admin", password="secret",
        )
        with pytest.raises(AttributeError):
            creds.host = "other"


# ===================================================================
# Host parsing utilities
# ===================================================================


class TestInferRegion:
    def test_provisioned(self):
        host = "my-cluster.abc123.us-east-1.redshift.amazonaws.com"
        assert infer_region(host) == "us-east-1"

    def test_serverless(self):
        host = "my-wg.123456789.eu-west-1.redshift-serverless.amazonaws.com"
        assert infer_region(host) == "eu-west-1"

    def test_ap_region(self):
        host = "cluster.abc.ap-southeast-1.redshift.amazonaws.com"
        assert infer_region(host) == "ap-southeast-1"

    def test_unknown_host(self):
        assert infer_region("localhost") is None

    def test_custom_domain(self):
        assert infer_region("redshift.internal.company.com") is None


class TestIsServerless:
    def test_provisioned(self):
        assert not is_serverless("my-cluster.abc.us-east-1.redshift.amazonaws.com")

    def test_serverless(self):
        assert is_serverless("wg.123.us-east-1.redshift-serverless.amazonaws.com")

    def test_case_insensitive(self):
        assert is_serverless("wg.123.us-east-1.Redshift-Serverless.amazonaws.com")


class TestExtractIdentifiers:
    def test_cluster_id(self):
        assert extract_cluster_identifier(
            "my-cluster.abc.us-east-1.redshift.amazonaws.com"
        ) == "my-cluster"

    def test_workgroup_name(self):
        assert extract_workgroup_name(
            "my-workgroup.123.us-east-1.redshift-serverless.amazonaws.com"
        ) == "my-workgroup"


# ===================================================================
# Password auth
# ===================================================================


class TestPasswordAuth:
    def test_basic(self):
        creds = resolve_credentials(
            host="host.example.com", user="admin", password="secret",
        )
        assert creds.host == "host.example.com"
        assert creds.user == "admin"
        assert creds.password == "secret"
        assert creds.port == 5439
        assert creds.database == "dev"

    def test_custom_port_and_database(self):
        creds = resolve_credentials(
            host="host", user="admin", password="secret",
            port=5440, database="prod",
        )
        assert creds.port == 5440
        assert creds.database == "prod"

    def test_missing_user(self):
        with pytest.raises(ValueError, match="user is required"):
            resolve_credentials(host="host", password="secret")

    def test_default_auth_type(self):
        creds = resolve_credentials(host="host", user="admin", password="secret")
        assert creds.user == "admin"

    def test_empty_password_allowed(self):
        creds = resolve_credentials(host="host", user="admin")
        assert creds.password == ""


class TestInvalidAuthType:
    def test_unknown(self):
        with pytest.raises(ValueError, match="Unknown auth_type"):
            resolve_credentials(host="host", auth_type="kerberos")


# ===================================================================
# Input validation
# ===================================================================


class TestInputValidation:
    def test_duration_too_low(self):
        with pytest.raises(ValueError, match="duration_seconds must be between"):
            resolve_credentials(
                host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
                auth_type="iam",
                db_user="admin",
                duration_seconds=100,
            )

    def test_duration_too_high(self):
        with pytest.raises(ValueError, match="duration_seconds must be between"):
            resolve_credentials(
                host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
                auth_type="iam",
                db_user="admin",
                duration_seconds=7200,
            )

    def test_invalid_secret_arn(self):
        with pytest.raises(ValueError, match="valid Secrets Manager ARN"):
            resolve_credentials(
                host="host",
                auth_type="secrets_manager",
                secret_arn="not-an-arn",
            )

    def test_repr_never_shows_password(self):
        creds = resolve_credentials(
            host="host", user="admin", password="super-secret-password",
        )
        text = repr(creds)
        assert "super-secret-password" not in text
        assert "***" in text

    def test_str_never_shows_password(self):
        creds = resolve_credentials(
            host="host", user="admin", password="super-secret-password",
        )
        text = str(creds)
        assert "super-secret-password" not in text


# ===================================================================
# IAM auth — provisioned
# ===================================================================


class TestIamProvisioned:
    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_basic(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:admin",
            "DbPassword": "temp-password-xyz",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            auth_type="iam",
            db_user="admin",
        )

        assert creds.user == "IAM:admin"
        assert creds.password == "temp-password-xyz"
        assert creds.host == "my-cluster.abc.us-east-1.redshift.amazonaws.com"
        assert creds.port == 5439
        assert creds.database == "dev"

        mock_client.get_cluster_credentials.assert_called_once_with(
            ClusterIdentifier="my-cluster",
            DbUser="admin",
            DbName="dev",
            DurationSeconds=900,
            AutoCreate=False,
        )

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_with_groups_and_autocreate(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:admin",
            "DbPassword": "temp-pass",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            auth_type="iam",
            db_user="admin",
            db_groups=["analysts", "admins"],
            auto_create=True,
            duration_seconds=3600,
        )

        call_kwargs = mock_client.get_cluster_credentials.call_args[1]
        assert call_kwargs["DbGroups"] == ["analysts", "admins"]
        assert call_kwargs["AutoCreate"] is True
        assert call_kwargs["DurationSeconds"] == 3600

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_db_user_falls_back_to_user(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:fallback_user",
            "DbPassword": "temp-pass",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            auth_type="iam",
            user="fallback_user",  # no db_user, should fall back
        )

        call_kwargs = mock_client.get_cluster_credentials.call_args[1]
        assert call_kwargs["DbUser"] == "fallback_user"

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_explicit_region(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:admin",
            "DbPassword": "temp-pass",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        resolve_credentials(
            host="my-cluster.abc.us-west-2.redshift.amazonaws.com",
            auth_type="iam",
            db_user="admin",
            region="us-west-2",
        )

        mock_session_factory.assert_called_once_with(
            region="us-west-2", profile=None,
        )

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_with_profile(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:admin",
            "DbPassword": "temp-pass",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            auth_type="iam",
            db_user="admin",
            profile="production",
        )

        mock_session_factory.assert_called_once_with(
            region="us-east-1", profile="production",
        )

    def test_missing_host(self):
        with pytest.raises(ValueError, match="host is required"):
            resolve_credentials(host="", auth_type="iam", db_user="admin")

    def test_missing_user(self):
        with pytest.raises(ValueError, match="db_user"):
            resolve_credentials(
                host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
                auth_type="iam",
            )

    def test_cannot_infer_region(self):
        with pytest.raises(ValueError, match="Cannot infer AWS region"):
            resolve_credentials(
                host="localhost", auth_type="iam", db_user="admin",
            )

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_uri_output_works_for_adbc(self, mock_session_factory):
        """Verify the resolved creds produce a valid URI for ADBC."""
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbUser": "IAM:admin",
            "DbPassword": "p@ss/w0rd!",
            "Expiration": "2026-04-23T12:00:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            auth_type="iam",
            db_user="admin",
        )

        uri = creds.as_uri()
        parsed = urlparse(uri)
        assert parsed.scheme == "postgresql"
        assert unquote(parsed.username) == "IAM:admin"
        assert unquote(parsed.password) == "p@ss/w0rd!"
        assert parsed.hostname == "my-cluster.abc.us-east-1.redshift.amazonaws.com"


# ===================================================================
# IAM auth — serverless
# ===================================================================


class TestIamServerless:
    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_basic(self, mock_session_factory):
        mock_client = MagicMock()
        mock_client.get_credentials.return_value = {
            "dbUser": "IAMR:admin",
            "dbPassword": "serverless-temp-pass",
            "expiration": "2026-04-23T12:00:00Z",
            "nextRefreshTime": "2026-04-23T11:45:00Z",
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="my-wg.123456789.eu-west-1.redshift-serverless.amazonaws.com",
            auth_type="iam",
            db_user="admin",
        )

        assert creds.user == "IAMR:admin"
        assert creds.password == "serverless-temp-pass"

        mock_client.get_credentials.assert_called_once_with(
            workgroupName="my-wg",
            dbName="dev",
        )
        mock_session.client.assert_called_with("redshift-serverless")


# ===================================================================
# Secrets Manager auth
# ===================================================================


class TestSecretsManager:
    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_basic(self, mock_session_factory):
        secret = {
            "username": "sm_user",
            "password": "sm_password",
            "host": "my-cluster.abc.us-east-1.redshift.amazonaws.com",
            "port": 5439,
            "dbname": "production",
            "engine": "redshift",
        }
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret),
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="placeholder",
            auth_type="secrets_manager",
            secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:my-secret",
        )

        assert creds.user == "sm_user"
        assert creds.password == "sm_password"
        assert creds.host == "my-cluster.abc.us-east-1.redshift.amazonaws.com"
        assert creds.port == 5439
        assert creds.database == "production"

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_fallback_to_params(self, mock_session_factory):
        """If secret doesn't have host/port, fall back to function params."""
        secret = {"username": "sm_user", "password": "sm_password"}
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret),
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
            port=5440,
            database="analytics",
            auth_type="secrets_manager",
            secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:s",
        )

        assert creds.host == "my-cluster.abc.us-east-1.redshift.amazonaws.com"
        assert creds.port == 5440
        assert creds.database == "analytics"

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_database_key_variant(self, mock_session_factory):
        """Some secrets use 'database' instead of 'dbname'."""
        secret = {"username": "user", "password": "pass", "database": "mydb"}
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret),
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        creds = resolve_credentials(
            host="host", auth_type="secrets_manager",
            secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:s",
        )
        assert creds.database == "mydb"

    def test_missing_arn(self):
        with pytest.raises(ValueError, match="secret_arn is required"):
            resolve_credentials(host="host", auth_type="secrets_manager")

    @patch("arrowjet.auth.redshift._get_boto3_session")
    def test_missing_username_in_secret(self, mock_session_factory):
        secret = {"password": "pass"}
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret),
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        mock_session_factory.return_value = mock_session

        with pytest.raises(ValueError, match="username"):
            resolve_credentials(
                host="host", auth_type="secrets_manager",
                secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:s",
            )


# ===================================================================
# boto3 import error
# ===================================================================


class TestBoto3Missing:
    @patch.dict("sys.modules", {"boto3": None})
    def test_iam_without_boto3(self):
        with pytest.raises(ImportError, match="boto3 is required"):
            resolve_credentials(
                host="my-cluster.abc.us-east-1.redshift.amazonaws.com",
                auth_type="iam",
                db_user="admin",
            )
