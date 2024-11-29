# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Request
from keycloak import KeycloakOpenID

from data_rentgen.db.models import User
from data_rentgen.dependencies import Stub
from data_rentgen.dto import UserDTO
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.exceptions.redirect import RedirectError
from data_rentgen.server.providers.auth.base_provider import AuthProvider
from data_rentgen.server.settings.auth.keycloack import KeycloakAuthProviderSettings
from data_rentgen.services import UnitOfWork

logger = logging.getLogger(__name__)


class KeycloakAuthProvider(AuthProvider):
    def __init__(
        self,
        settings: Annotated[KeycloakAuthProviderSettings, Depends(Stub(KeycloakAuthProviderSettings))],
        unit_of_work: Annotated[UnitOfWork, Depends()],
    ) -> None:
        self.settings = settings
        self._uow = unit_of_work
        self.keycloak_openid = KeycloakOpenID(
            server_url=self.settings.keycloak.server_url,
            client_id=self.settings.keycloak.client_id,
            realm_name=self.settings.keycloak.realm_name,
            client_secret_key=self.settings.keycloak.client_secret.get_secret_value(),
            verify=self.settings.keycloak.verify_ssl,
        )

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = KeycloakAuthProviderSettings.model_validate(app.state.settings.auth.dict(exclude={"provider"}))
        logger.info("Using %s provider with settings:\n%s", cls.__name__, settings)
        app.dependency_overrides[AuthProvider] = cls
        app.dependency_overrides[KeycloakAuthProviderSettings] = lambda: settings
        return app

    async def get_token_password_grant(
        self,
        grant_type: str | None = None,
        login: str | None = None,
        password: str | None = None,
        scopes: list[str] | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError("Password grant is not supported by KeycloakAuthProvider.")

    async def get_token_authorization_code_grant(
        self,
        code: str,
        redirect_uri: str,
        scopes: list[str] | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> dict[str, Any]:
        try:
            redirect_uri = redirect_uri or self.settings.keycloak.redirect_uri
            return self.keycloak_openid.token(
                grant_type="authorization_code",
                code=code,
                redirect_uri=redirect_uri,
            )
        except Exception as e:
            raise AuthorizationError("Failed to get token") from e

    async def get_current_user(self, access_token: str, *args, **kwargs) -> User:
        request: Request = kwargs["request"]
        refresh_token = request.session.get("refresh_token")

        if not access_token:
            logger.debug("No access token found in session.")
            self.redirect_to_auth()

        # if user is disabled or blocked in Keycloak after the token is issued, he will
        # remain authorized until the token expires (not more than 15 minutes in MTS SSO)
        token_info = self.decode_token(access_token)

        if token_info is None and refresh_token:
            logger.debug("Access token invalid. Attempting to refresh.")
            access_token, refresh_token = self.refresh_access_token(refresh_token)
            request.session["access_token"] = access_token
            request.session["refresh_token"] = refresh_token

            token_info = self.decode_token(access_token)

            if token_info is None:
                # If there is no token_info after refresh user get redirect
                self.redirect_to_auth()

        # these names are hardcoded in keycloak:
        # https://github.com/keycloak/keycloak/blob/3ca3a4ad349b4d457f6829eaf2ae05f1e01408be/core/src/main/java/org/keycloak/representations/IDToken.java
        user_id = token_info.get("sub")  # type: ignore[union-attr]
        login = token_info.get("preferred_username")  # type: ignore[union-attr]
        if not user_id:
            raise AuthorizationError("Invalid token payload")
        return await self._uow.user.get_or_create(UserDTO(name=login))  # type: ignore[arg-type]

    def decode_token(self, access_token: str) -> dict[str, Any] | None:
        try:
            return self.keycloak_openid.decode_token(token=access_token)
        except Exception as err:
            logger.info("Access token is invalid or expired: %s", err)
            return None

    def refresh_access_token(self, refresh_token: str) -> tuple[str, str]:  # type: ignore[return]
        try:
            new_tokens = self.keycloak_openid.refresh_token(refresh_token)
            logger.debug("Access token refreshed")
            return new_tokens.get("access_token"), new_tokens.get("refresh_token")
        except Exception as err:
            logger.debug("Failed to refresh access token: %s", err)
            self.redirect_to_auth()

    def redirect_to_auth(self) -> None:
        auth_url = self.keycloak_openid.auth_url(
            redirect_uri=self.settings.keycloak.redirect_uri,
            scope=self.settings.keycloak.scope,
        )
        raise RedirectError(message=auth_url, details="Authorize on provided url")
