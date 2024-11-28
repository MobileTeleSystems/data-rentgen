# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import secrets

state_store: dict[str, str] = {}


def generate_state(redirect_url: str) -> str:
    state = secrets.token_urlsafe(16)  # noqa: WPS432
    state_store[state] = redirect_url
    return state


def validate_state(state: str) -> str | None:
    return state_store.pop(state, None)
