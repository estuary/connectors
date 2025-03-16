#!/bin/bash

SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' -E 's|(/)[0-9a-fA-F-]{36}/[0-9a-fA-F-]{36}(\.)|\1<uuid>/<uuid>\2|g' ${SNAPSHOT}
$SED_CMD -i'' "s|_${TABLE_SUFFIX}||g" ${SNAPSHOT}
