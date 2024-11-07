# cleanup the snapshot by replacing s3 paths and bucket with a placeholder so that the test is reproducible
SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi

$SED_CMD -i'' 's/"Bucket": "[^"]*"/"Bucket": "<test_bucket>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"[^"]*\.json"/"<test_file>"/g' ${SNAPSHOT}
$SED_CMD -i'' "s/'[^']*\.json'/'<test_file>'/g" ${SNAPSHOT}
