import json
import time
from base64 import b64encode

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from itsdangerous import TimestampSigner

from data_rentgen.server.settings.auth.keycloak import KeycloakSettings


@pytest.fixture(scope="session")
def rsa_keys():
    # create private & public keys to emulate Keycloak signing
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_key = private_key.public_key()

    return {
        "private_key": private_key,
        "private_pem": private_pem,
        "public_key": public_key,
    }


def get_public_key_pem(public_key):
    public_pem = public_key.public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )
    public_pem_str = public_pem.decode("utf-8")
    public_pem_str = public_pem_str.replace("-----BEGIN PUBLIC KEY-----\n", "")
    public_pem_str = public_pem_str.replace("-----END PUBLIC KEY-----\n", "")
    public_pem_str = public_pem_str.replace("\n", "")
    return public_pem_str


@pytest.fixture
def create_session_cookie(rsa_keys, server_app_settings):
    def _create_session_cookie(user, expire_in_msec=5000) -> str:
        private_pem = rsa_keys["private_pem"]
        session_secret_key = server_app_settings.server.session.secret_key

        payload = {
            "sub": str(user.id),
            "preferred_username": user.name,
            "email": "user@example.com",
            "given_name": "first_name",
            "middle_name": "middle_name",
            "family_name": "family_name",
            "exp": int(time.time()) + (expire_in_msec / 1000),
        }

        access_token = jwt.encode(
            payload=payload,
            key=private_pem,
            algorithm="RS256",
        )
        refresh_token = "mock_refresh_token"

        session_data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
        }

        signer = TimestampSigner(session_secret_key)
        json_bytes = json.dumps(session_data).encode("utf-8")
        base64_bytes = b64encode(json_bytes)
        signed_data = signer.sign(base64_bytes)
        session_cookie = signed_data.decode("utf-8")

        return session_cookie

    return _create_session_cookie


@pytest.fixture
def mock_keycloak_well_known(server_app_settings, respx_mock):
    keycloak_settings = KeycloakSettings.model_validate(server_app_settings.auth.keycloak)
    server_url = keycloak_settings.server_url
    realm_name = keycloak_settings.realm_name

    respx_mock.get(f"{server_url}/realms/{realm_name}/.well-known/openid-configuration").respond(
        status_code=200,
        json={
            "authorization_endpoint": f"{server_url}/realms/{realm_name}/protocol/openid-connect/auth",
            "token_endpoint": f"{server_url}/realms/{realm_name}/protocol/openid-connect/token",
            "userinfo_endpoint": f"{server_url}/realms/{realm_name}/protocol/openid-connect/userinfo",
            "end_session_endpoint": f"{server_url}/realms/{realm_name}/protocol/openid-connect/logout",
            "jwks_uri": f"{server_url}/realms/{realm_name}/protocol/openid-connect/certs",
            "issuer": f"{server_url}/realms/{realm_name}",
        },
        content_type="application/json",
    )


@pytest.fixture
def mock_keycloak_realm(server_app_settings, rsa_keys, respx_mock):
    keycloak_settings = KeycloakSettings.model_validate(server_app_settings.auth.keycloak)
    server_url = keycloak_settings.server_url
    realm_name = keycloak_settings.realm_name
    public_pem_str = get_public_key_pem(rsa_keys["public_key"])

    respx_mock.get(f"{server_url}/realms/{realm_name}").respond(
        status_code=200,
        json={
            "realm": realm_name,
            "public_key": public_pem_str,
            "token-service": f"{server_url}/realms/{realm_name}/protocol/openid-connect/token",
            "account-service": f"{server_url}/realms/{realm_name}/account",
        },
        content_type="application/json",
    )


@pytest.fixture
def mock_keycloak_token_refresh(user, server_app_settings, rsa_keys, respx_mock):
    keycloak_settings = KeycloakSettings.model_validate(server_app_settings.auth.keycloak)
    server_url = keycloak_settings.server_url
    realm_name = keycloak_settings.realm_name

    # generate new access and refresh tokens
    expires_in = int(time.time()) + 5000
    private_pem = rsa_keys["private_pem"]
    payload = {
        "sub": str(user.id),
        "preferred_username": user.name,
        "email": "mock_email@example.com",
        "given_name": "Mock",
        "middle_name": "User",
        "family_name": "Name",
        "exp": expires_in,
    }

    new_access_token = jwt.encode(
        payload=payload,
        key=private_pem,
        algorithm="RS256",
    )
    new_refresh_token = "mock_new_refresh_token"

    respx_mock.post(f"{server_url}/realms/{realm_name}/protocol/openid-connect/token").respond(
        status_code=200,
        json={
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
            "expires_in": expires_in,
        },
        content_type="application/json",
    )
