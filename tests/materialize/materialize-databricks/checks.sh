# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
sed -i='' 's/.\{36\}\.json/<uuid>/g' ${SNAPSHOT}
