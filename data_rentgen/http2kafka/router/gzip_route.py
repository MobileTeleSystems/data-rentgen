# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import zlib
from collections.abc import Callable
from http import HTTPStatus

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

# https://stackoverflow.com/a/22311297/23601543
GZIP = 16 | zlib.MAX_WBITS


class SupportsGzipRequest(Request):
    # https://fastapi.tiangolo.com/how-to/custom-request-and-route/#create-a-custom-gziprequest-class
    async def body(self) -> bytes:
        if hasattr(self, "_body"):
            return self._body

        content_encoding = self.headers.getlist("Content-Encoding")
        if "gzip" in content_encoding:
            decompressor = zlib.decompressobj(GZIP)
        elif content_encoding:
            msg = f"Unsupported Content-Encoding: {content_encoding}"
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=msg)
        else:
            return await super().body()

        chunks = [decompressor.decompress(chunk) async for chunk in self.stream()]
        chunks.append(decompressor.flush())
        self._body = b"".join(chunks)
        return self._body


class SupportsGzipRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            request = SupportsGzipRequest(request.scope, request.receive)
            return await original_route_handler(request)

        return custom_route_handler
