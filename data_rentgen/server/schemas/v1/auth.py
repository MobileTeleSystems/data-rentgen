# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel


class TokenPayloadSchema(BaseModel):
    user_id: int


class AuthTokenSchema(BaseModel):
    access_token: str
    token_type: str
    expires_at: float
