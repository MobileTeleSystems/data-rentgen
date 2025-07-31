#!/bin/env python3
# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import json
import sys

from fastapi import FastAPI

from data_rentgen.http2kafka import get_application


def get_openapi_schema(app: FastAPI) -> dict:
    return app.openapi()


if __name__ == "__main__":
    app = get_application()
    schema = get_openapi_schema(app)
    file_path = sys.argv[1]
    with open(file_path, "w") as file:  # noqa: PTH123
        json.dump(schema, file)
