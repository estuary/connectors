# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
sed -i '' 's/"JobID": ".*"/"Path": "<uuid>"/g' ${SNAPSHOT}
sed -i '' 's/"gs:\/\/.*"/"<path>"/g' ${SNAPSHOT}
sed -i '' 's/".\{36\}\/.\{36\}"/"<path>"/g' ${SNAPSHOT}
