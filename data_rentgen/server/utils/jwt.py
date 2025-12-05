# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import jwt

from data_rentgen.exceptions.auth import AuthorizationError


def sign_jwt(payload: dict, secret_key: str, security_algorithm: str) -> str:
    payload = payload.copy()
    payload["iss"] = "data-rentgen"
    return jwt.encode(
        payload=payload,
        key=secret_key,
        algorithm=security_algorithm,
    )


def decode_jwt(token: str, secret_key: str, security_algorithm: str) -> dict:
    try:
        claims = jwt.decode(
            jwt=token,
            key=secret_key,
            algorithms=[security_algorithm],
            issuer="data-rentgen",
        )

        if "exp" not in claims:
            err_msg = "Missing expiration time in token"
            raise jwt.ExpiredSignatureError(err_msg)

    except jwt.exceptions.ExpiredSignatureError as e:
        err_msg = "Invalid token"
        details = "Token has expired"
        raise AuthorizationError(err_msg, details=details) from e
    except jwt.PyJWTError as e:
        err_msg = "Invalid token"
        raise AuthorizationError(err_msg, details=e.args[0]) from e

    return claims
