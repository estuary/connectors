# BigQuery
This materialization allows you to store data in Google BigQuery. It supports both Delta (Journaling)
and Non-Delta mode where records are upserted. 

The system is designed to use a service account to access BigQuery and Google Cloud Storage.
Google Cloud Storage is leveraged as a source for external table data which is joined with BigQuery data 
for retrieving and storing data.

## Permissions Required
* Role BigQuery.dataEditor - On the dataset where you want to store your tables.
* Role BigQuery.Jobs User - Per project where you BigQuery database is located.
* Role Cloud Storage.Storage Object Admin - On temp bucket where external table data will be held.

## Other Considerations
- The Google Cloud Storage bucket needs to be in the same region as the BigQuery dataset/tables. It must be set at create time and cannot be changed. 
- Files in the temporary bucket should be automatically deleted but feel free to set a lifecyle policy on the bucket to ensure no leakage in case of container restarts. 24 hours is more than enough to parse the data.
- Dataset names are case-sensitive: mydataset and MyDataset can coexist in the same project.
- Dataset names cannot contain spaces or special characters such as -, &, @, or %.

## Performance
The BigQuery connector loads and stores blocks of documents. The processing time required to run these transactions seems to be pretty static 
for smaller transactions. (About 10 seconds) You will see better performance by using longer/larger transaction/checkpoint times as many small
transactions will take much longer than fewer large transactions. Setting the minimum transaction time to larger values (minutes) will likely 
yield much better performance. 

# Configuration
The configuration required:

```
project_id - The project ID for the Google Cloud Storage bucket and BigQuery Dataset
region - The Google region.
bucket - The Google Cloud Storage bucket name
bucket_path - The base path to store temporary files
credentials_file - The path to a JSON service account file
credentials_json - Base64 encoded string of the full service account file
```

You should specify one of `credentials_file` or `credentials_json`. It will also leverage 
the `GOOGLE_APPLICATION_CREDENTIALS` environment variable if provided which can point
to a service account file. If multiple options are listed, tt will first try `credentials_file`
followed by `credentials_json` and then any provided `GOOGLE_APPLICATION_CREDENTIALS`.
