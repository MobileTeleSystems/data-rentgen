# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from functools import partial

from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.openapi.utils import get_openapi
from starlette.requests import Request
from starlette.responses import JSONResponse

from data_rentgen.server.settings.openapi import OpenAPISettings


async def custom_openapi(request: Request) -> JSONResponse:
    app: FastAPI = request.app
    root_path = request.scope.get("root_path", "").rstrip("/")
    server_urls = set(filter(None, (server_data.get("url") for server_data in app.servers)))

    if root_path and app.root_path_in_servers and root_path not in server_urls:
        app.servers.insert(0, {"url": root_path})
        server_urls.add(root_path)

    return JSONResponse(app.openapi())


def custom_openapi_schema(app: FastAPI, settings: OpenAPISettings) -> dict:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        summary=app.summary,
        description=app.description,
        terms_of_service=app.terms_of_service,
        contact=app.contact,
        license_info=app.license_info,
        routes=app.routes,
        webhooks=app.webhooks.routes,
        tags=app.openapi_tags,
        servers=app.servers,
        separate_input_output_schemas=app.separate_input_output_schemas,
    )
    # https://redocly.com/docs/api-reference-docs/specification-extensions/x-logo/
    openapi_schema["info"]["x-logo"] = {
        "url": str(settings.logo.url),
        "altText": str(settings.logo.alt_text),
        "backgroundColor": f"#{settings.logo.background_color}",
        "href": str(settings.logo.href),
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


async def custom_swagger_ui_html(request: Request):
    app: FastAPI = request.app
    settings: OpenAPISettings = app.state.settings.server.openapi
    root_path = request.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + request.app.openapi_url  # type: ignore[arg-type]
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=f"{app.title} - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url=str(settings.swagger.js_url),
        swagger_css_url=str(settings.swagger.css_url),
        swagger_favicon_url=str(settings.favicon.url),
    )


async def custom_swagger_ui_redirect(request: Request):
    return get_swagger_ui_oauth2_redirect_html()


async def custom_redoc_html(request: Request):
    app: FastAPI = request.app
    settings: OpenAPISettings = app.state.settings.server.openapi
    root_path = request.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + request.app.openapi_url  # type: ignore[arg-type]
    return get_redoc_html(
        openapi_url=openapi_url,
        title=f"{app.title} - ReDoc",
        redoc_js_url=settings.redoc.js_url,
        redoc_favicon_url=settings.favicon.url,
        with_google_fonts=False,
    )


def apply_openapi_middleware(app: FastAPI, settings: OpenAPISettings) -> FastAPI:
    """Add OpenAPI middleware to the application."""
    # https://fastapi.tiangolo.com/how-to/custom-docs-ui-assets/#include-the-custom-docs
    if settings.enabled:
        app.openapi_url = "/openapi.json"
        app.add_route(app.openapi_url, custom_openapi, include_in_schema=False)

    if settings.swagger.enabled:
        app.docs_url = "/docs"
        app.swagger_ui_oauth2_redirect_url = "/docs/oauth2-redirect"
        app.add_route(app.docs_url, custom_swagger_ui_html, include_in_schema=False)
        app.add_route(app.swagger_ui_oauth2_redirect_url, custom_swagger_ui_redirect, include_in_schema=False)

    if settings.redoc.enabled:
        app.redoc_url = "/redoc"
        app.add_route(app.redoc_url, custom_redoc_html, include_in_schema=False)

    app.openapi = partial(custom_openapi_schema, app=app, settings=settings)  # type: ignore[method-assign]
    return app
