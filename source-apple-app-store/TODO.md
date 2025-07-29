# Apple App Store Connector - Checklist

## Status

This connector is still in development since we haven't been able to see what the Google Play data looks like without valid credentials.

## What's been tested

Not much can be tested without credentials unfortunately. A capture can be created, run discovery, and run without fetching any data - assuming proper credentials are provided.
If private key is invalid the JWT token generation fails.

## What needs to be done

- `AppleAppStoreClient.stream_tsv_data`
  - Works as expected
  - What is data encoding
  - What can/cannot be null
  - What are the unique identifierers in the data
- The `_add_row_number` and `parse_by_field_type` before model and field validators
  - Implement actual parsing logic for fields as needed
  - Does it work in general
  - Are the values in the model correct for empty strings, dates, booleans, etc.
  - Any undocumented fields that are useful
- Do the report requests or other api endpoints return data about when a report request was created?
- Does the cooldown time for ONE_TIME_SNAPSHOTS hold water? What happens if we try to create a new one close to the cooldown time and there already exists one?

## Authentication Testing
- [ ] Test JWT token generation with real credentials
- [ ] Validate JWT token refresh works

## Production Readiness
- [ ] Create official documentation in Flow repo
- [ ] Update connector with newly discovered information