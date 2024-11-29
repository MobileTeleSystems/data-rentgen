.. _auth-server-keycloak:

Keycloak Provider
===================

Description
-----------

Keycloak auth provider uses `python-keycloak <https://pypi.org/project/python-keycloak/>` library to interact with Keycloak server. During the authentication process,
KeycloakAuthProvider redirects user to Keycloak authentication page.

After successful authentication, Keycloak redirects user back to Data.Rentgen with authorization code.
Then KeycloakAuthProvider exchanges authorization code for an access token and uses it to get user information from Keycloak server.
If user is not found in Data.Rentgen database, KeycloakAuthProvider creates it. Finally, KeycloakAuthProvider returns user with access token.

Interaction schema
------------------

.. dropdown:: Interaction schema

    .. plantuml::

        @startuml
            title DummyAuthProvider
            participant "Client"
            participant "Backend"

            == GET v1/datasets ==

            alt Successful case
                "Client" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Client" -- : Return data

            else Token is expired (Successful case)
                "Client" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" -[#yellow]> "Backend" : Token is expired
                "Backend" --> "Backend" : Try to refresh token
                "Backend" --> "Backend" : Validate new token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Client" -- : Return data

            else Create new User
                "Client" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" --> "Backend" : Create new user
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Client" -- : Return data

            else Token is expired and bad refresh token
                "Client" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" -[#yellow]> "Backend" : Token is expired
                "Backend" --> "Backend" : Try to refresh token
                "Backend" x-[#red]> "Client" -- : RedirectResponse can't refresh

            else Bad Token payload
                "Client" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" x-[#red]> "Client" -- : 307 Authorization error

            end

            deactivate "Client"
        @enduml


Basic Configuration
-------------------

.. autopydantic_model:: data_rentgen.server.settings.auth.keycloak.KeycloakAuthProviderSettings
.. autopydantic_model:: data_rentgen.server.settings.auth.keycloak.KeycloakSettings


