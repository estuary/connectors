# cleanup the snapshot by replacing uuids with a placeholder so that the test is reproducible
SED_CMD="sed"
if [ "$(uname -s)" = "Darwin" ]; then
  SED_CMD="gsed"
fi
$SED_CMD -i'' 's/@flow_v1\/.\{36\}/<uuid>/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"Path": ".*"/"Path": "<uuid>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"PipeStartTime": ".\{1,\}"/"PipeStartTime": "<timestamp>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"path": ".*"/"path": "<path>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"md5": ".*"/"md5": "<md5>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"chunk_length": [0-9]\+/"chunk_length": "<chunk_length>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"chunk_length_uncompressed": [0-9]\+/"chunk_length_uncompressed": "<chunk_length_uncompressed>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"chunk_md5": ".*"/"chunk_md5": "<chunk_md5>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"encryption_key_id": [0-9]\+/"encryption_key_id": "<encryption_key_id>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"first_insert_time_in_ms": [0-9]\+/"first_insert_time_in_ms": "<first_insert_time_in_ms>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"last_insert_time_in_ms": [0-9]\+/"last_insert_time_in_ms": "<last_insert_time_in_ms>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"flush_start_ms": [0-9]\+/"flush_start_ms": "<flush_start_ms>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"build_duration_ms": [0-9]\+/"build_duration_ms": "<build_duration_ms>"/g' ${SNAPSHOT}
$SED_CMD -i'' 's/"upload_duration_ms": [0-9]\+/"upload_duration_ms": "<upload_duration_ms>"/g' ${SNAPSHOT}
# Replace transient staging table names (FLOW_STAGING_INSERT_<idx>_<uuid> and FLOW_STAGING_MERGE_<idx>_<uuid>)
$SED_CMD -i'' 's/FLOW_STAGING_INSERT_[0-9]\+_[a-f0-9_]\{36\}/FLOW_STAGING_INSERT_<binding>_<uuid>/g' ${SNAPSHOT}
$SED_CMD -i'' 's/FLOW_STAGING_MERGE_[0-9]\+_[a-f0-9_]\{36\}/FLOW_STAGING_MERGE_<binding>_<uuid>/g' ${SNAPSHOT}
