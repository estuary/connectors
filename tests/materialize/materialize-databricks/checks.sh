# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' 's/.\{36\}\.json/<uuid>/g' ${SNAPSHOT}
