#
# Copyright (c) 2023 Estuary, Inc., all rights reserved.
#

from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.11",
]

setup(
    name = "source_zoom",
    description = "Source implementation for Zoom.",
    author = "Estuary",
    author_email = "contact@estuary.dev",
    packages = find_packages(),
    install_requires = MAIN_REQUIREMENTS,
    package_data = { "": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"] },
)