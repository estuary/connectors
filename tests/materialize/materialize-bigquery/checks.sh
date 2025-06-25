SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' \
  -e 's/"JobPrefix": "[^"]*"/"JobPrefix": "<uuid>"/g' \
  -e 's|"gs://[^/]*/[^"]*"|"gs://[bucket]/<uuid>"|g' \
  ${SNAPSHOT}
