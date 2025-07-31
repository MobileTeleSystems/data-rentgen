#!/bin/env python3
# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import sys
from pathlib import Path

import uvicorn

here = Path(__file__).resolve()


def main(prog_name: str | None = None, args: list[str] | None = None):
    """Run uvicorn and pass the command line arguments to it."""
    if args is None:
        args = sys.argv.copy()
        prog_name = args.pop(0)

    if not prog_name:
        prog_name = os.fspath(here)

    args = args.copy()
    # prepend config path before command line arguments
    args.insert(0, "--factory")
    args.insert(1, "data_rentgen.http2kafka:get_application")

    # call uvicorn
    uvicorn.main.main(args=args, prog_name=prog_name)


if __name__ == "__main__":
    main()
