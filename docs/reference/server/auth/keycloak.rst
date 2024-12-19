.. _auth-server-keycloak:

Keycloak Provider
===================

Description
-----------

Keycloak auth provider uses `python-keycloak <https://pypi.org/project/python-keycloak/>`_ library to interact with Keycloak server. During the authentication process,
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
            participant "Keycloak"

            == Client Authentication at Keycloak ==

            Client -> Backend : Request endpoint with authentication (/locations)

            Backend x-[#red]> Client: 401 with redirect url in 'details' response field

            Client -> Keycloak : Redirect user to Keycloak login page

            alt Successful login
                Client --> Keycloak : Log in with login and password
            else Login failed
                Keycloak x-[#red]> Client -- : Display error (401 Unauthorized)
            end

            Keycloak -> Client : Callback to Client /callback which is proxy between Keycloak and Backend

            Client -> Backend : Send request to Backend '/v1/auth/callback'

            Backend -> Keycloak : Check original 'state' and exchange code for token's
            Keycloak --> Backend : Return token's
            Backend --> Client : Set token's in user's browser in cookies and redirect /locations

            Client --> Backend : Redirect to /locations
            Backend -> Backend : Get user info from token and check user in internal backend database
            Backend -> Backend : Create user in internal backend database if not exist
            Backend -[#green]> Client -- : Return requested data


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
                "Backend" --> "Keycloak" : Try to refresh token
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
                "Backend" --> "Keycloak" : Try to refresh token
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


