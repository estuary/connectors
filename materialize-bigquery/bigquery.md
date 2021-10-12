# BigQuery
This materialization allows you to store data in Google BigQuery. It supports both Delta (Journaling)
and Non-Delta mode where records are updated. 

The system is designed to use a service account to access BigQuery and Google Cloud Storage.
Google Cloud Storage is leveraged as a source for external table data which is joined with BigQuery data 
for retrieving and storing data.

# Permissions Required
Role BigQuery.dataEditor - On the dataset where you want to store your tables
Role BigQuery.Jobs User - Per project
Role Cloud Storage.Storage Object Admin - On temp bucket where external table data will be held.

# Other Considerations
The Google Cloud Storage bucket needs to be in the same region as the BigQuery dataset/tables. It
must be set at create time and cannot be changed. 
Dataset names are case-sensitive: mydataset and MyDataset can coexist in the same project.
Dataset names cannot contain spaces or special characters such as -, &, @, or %.

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
to a service account file.