#!/bin/env python3
# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import sys
from pathlib import Path

from faststream.cli import cli

here = Path(__file__).resolve()


def main(prog_name: str | None = None, args: list[str] | None = None):
    """Run FastStream and pass the command line arguments to it."""
    if args is None:
        args = sys.argv.copy()
        prog_name = args.pop(0)

    if not prog_name:
        prog_name = os.fspath(here)

    args = args.copy()
    # prepend config path before command line arguments
    args.insert(0, "run")
    args.insert(1, "data_rentgen.consumer:get_application")
    args.insert(2, "--factory")

    # call uvicorn
    cli(args, prog_name=prog_name)


if __name__ == "__main__":
    main()
