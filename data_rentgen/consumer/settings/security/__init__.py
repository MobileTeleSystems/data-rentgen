# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from pydantic import Discriminator

from data_rentgen.consumer.settings.security.anonymous import KafkaSecurityAnonymousSettings
from data_rentgen.consumer.settings.security.gssapi import KafkaSecurityGSSAPISettings
from data_rentgen.consumer.settings.security.plain import KafkaSecurityPlaintextSettings
from data_rentgen.consumer.settings.security.scram import KafkaSecurityScram256Settings, KafkaSecurityScram512Settings

KafkaSecuritySettings = Annotated[
    KafkaSecurityAnonymousSettings
    | KafkaSecurityScram256Settings
    | KafkaSecurityScram512Settings
    | KafkaSecurityPlaintextSettings
    | KafkaSecurityGSSAPISettings,
    Discriminator("type"),
]

__all__ = [
    "KafkaSecurityAnonymousSettings",
    "KafkaSecurityGSSAPISettings",
    "KafkaSecurityPlaintextSettings",
    "KafkaSecurityScram256Settings",
    "KafkaSecurityScram512Settings",
    "KafkaSecuritySettings",
]
