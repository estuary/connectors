# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
sed -i '' 's/@flow_v1\/.\{36\}/<uuid>/g' ${SNAPSHOT}
sed -i '' 's/"Path": ".*"/"Path": "<uuid>"/g' ${SNAPSHOT}
sed -i '' 's/"PipeStartTime": ".\{1,\}"/"PipeStartTime": "<timestamp>"/g' ${SNAPSHOT}
