SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' 's/"uuid": ".\{36\}\"/"uuid": "<uuid>"/g' ${SNAPSHOT}
