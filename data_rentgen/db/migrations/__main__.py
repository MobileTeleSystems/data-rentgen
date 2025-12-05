#!/bin/env python3

# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import sys
from pathlib import Path

from alembic.config import CommandLine

here = Path(__file__).resolve()
config_path = here.parent / "alembic.ini"


def main(prog_name: str | None = None, args: list[str] | None = None):
    """Run alembic and pass the command line arguments to it."""
    if args is None:
        args = sys.argv.copy()
        prog_name = args.pop(0)

    if not prog_name:
        prog_name = os.fspath(here)

    # prepend config path before command line arguments
    args = args.copy()
    args.insert(0, "-c")
    args.insert(1, os.fspath(config_path))

    # call alembic
    cmd = CommandLine(prog=prog_name)
    cmd.main(argv=args)


if __name__ == "__main__":
    main()
