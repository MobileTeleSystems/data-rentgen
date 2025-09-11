# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

VERSION_FILE = Path(__file__).parent / "VERSION"
# version always contain only release number like 0.0.1
__version__ = VERSION_FILE.read_text().strip()
# version tuple always contains only integer parts, like (0, 0, 1)
__version_tuple__ = tuple(map(int, __version__.split(".")))  # noqa: RUF048
