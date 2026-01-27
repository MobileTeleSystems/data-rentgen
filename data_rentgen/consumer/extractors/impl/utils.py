# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
def parse_kv_tag(key: str):
    # https://github.com/apache/airflow/issues/61069
    if ":" not in key:
        return key, "true"

    key, value, *_ = key.split(":")
    return key, value
