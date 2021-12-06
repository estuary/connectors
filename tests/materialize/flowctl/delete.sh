#!/bin/bash
#
# This script builds and activates the test catalog.

set -e
flowctl api delete --build-id ${BUILD_ID} --all