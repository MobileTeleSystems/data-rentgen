#!/bin/env python3
# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from pathlib import Path
from typing import List, Optional

import uvicorn

here = Path(__file__).resolve()


def main(prog_name: Optional[str] = None, args: Optional[List[str]] = None):
    """Run uvicorn and pass the command line arguments to it."""
    if args is None:
        args = sys.argv.copy()
        prog_name = args.pop(0)

    if not prog_name:
        prog_name = os.fspath(here)

    args = args.copy()
    # prepend config path before command line arguments
    args.insert(0, "--factory")
    args.insert(1, "arrakis.backend:get_application")

    # call uvicorn
    uvicorn.main.main(args=args, prog_name=prog_name)


if __name__ == "__main__":
    main()
