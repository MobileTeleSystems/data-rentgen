# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import jwt

from data_rentgen.exceptions.auth import AuthorizationError


def sign_jwt(payload: dict, secret_key: str, security_algorithm: str) -> str:
    return jwt.encode(
        payload=payload,
        key=secret_key,
        algorithm=security_algorithm,
    )


def decode_jwt(token: str, secret_key: str, security_algorithm: str) -> dict:
    try:
        claims = jwt.decode(jwt=token, key=secret_key, algorithms=[security_algorithm])

        if "exp" not in claims:
            raise jwt.ExpiredSignatureError("Missing expiration time in token")

        return claims
    except jwt.PyJWTError as e:
        raise AuthorizationError("Invalid token") from e
