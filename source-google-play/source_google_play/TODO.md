# Status

This connector is still in development since we haven't been able to see what the Google Play data looks like without valid credentials.

# What's been tested
- `GCSClient.list_all_files` does list all files in a GCS bucket.
  - The `prefix` and `globPattern` inputs work to filter what files are returned.
- The `GoogleServiceAccount` credentials work with a valid service account JSON to make successful requests to GCS.
- A capture can be created. It won't yield any documents, but we'll be able to finish development once we have an active capture created.


# What hasn't been tested / outstanding questions
- `GCSClient.stream_csv`
  - What's the dialect for the CSVs that are in the GCS bucket? Do we need to pass a custom `CSVConfig` into the `IncrementalCSVProcessor`?
- The `_add_row_number` and `_extract_year_month` before model validators.
  - Do these work in general?
  - Are they inserting the correct values into each record?
  - Are there other, undocumented fields already in the records that contain the same data these model validators add?
- Generally, what do the CSVs look like and what fields are always present in each row?
  - Are the CSVs named like the Google Play docs say they are?
  - Is there some nice `id` type field we can use as a unique identifier for a row across all CSVs?
  - For `Statistics`, in the CSV for the current month, are rows for previous days no longer updated? Are we able improve the incremental strategy for these streams by only yielding rows for the same date as the current log cursor?
  - For `Reviews`, in the CSV for the current month, is there an always populated field like `review_last_update_date_and_time` that has fine enough grain that we could use it as a cursor field?