#
# Copyright (c) 2023 Estuary, Inc., all rights reserved.
#

from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.11",
    "msal~=1.25.0",
    "Office365-REST-Python-Client~=2.5.2",
    "smart-open~=6.4.0",
]

setup(
    name = "source_microsoft_sharepoint",
    description = "Source implementation for Microsoft Sharepoint.",
    author = "Estuary",
    author_email = "contact@estuary.dev",
    packages = find_packages(),
    install_requires = MAIN_REQUIREMENTS,
    package_data = { "": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"] },
)