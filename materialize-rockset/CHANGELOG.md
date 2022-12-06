# materialize-rockset

## v2, 2022-12-06
- Add required field `region_base_url` to configuration for setting the base URL. Previously this
  would only use `api.rs2.usw2.rockset.com`.
- Remove advanced fields `event_time_info` & `insert_only`. These have been deprecated by Rockset
  and are no longer used.
- Mark `workspace` and `collection` as required in the connector spec as these fields have always
  been required for a spec to validate.

## v1, 2022-07-27
- Beginning of changelog.
