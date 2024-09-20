# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' 's/@flow_v1\/.\{36\}/<uuid>/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"Path": ".*"/"Path": "<uuid>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"PipeStartTime": ".\{1,\}"/"PipeStartTime": "<timestamp>"/g' ${SNAPSHOT}
