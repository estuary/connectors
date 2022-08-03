# source-firestore


Firestore source requires that there is a monotonically increasing, unique field for each document that signifies their last update timestamp. This allows Flow to recognise changes in documents and addition of new documents in a fault-tolerant way. This field must be specified as `cursorField` when defining the bindings.

Example:
```
collections:
  acmeCo/users:
    schema: users.schema.yaml
    key: [/__name__]
captures:
  acmeCo/source-firestore:
    endpoint:
      connector:
        image: source-firestore:local
        config: source-firestore.config.yaml
    bindings:
      - resource:
          stream: users
          syncMode: incremental
          cursorField: [updated_at]
        target: acmeCo/users
```

# add_timestamp_field script

There is a script available for adding `updated_at` field to your collection, however note that **this script will only add the field once**, and it is necessary for any software interacting with the Firestore collections to update the `updated_at` field whenever the documents are created or updated, otherwise Flow will not be able to recognise those new documents or updates. The field is filled using `ServerTimestamp`, and so all updates to this field must also be done using the same method.

To use the script:

```
add-timestamp-field --credentials=path/to/credentials.json -c your_first_collection -c your_second_collection
```
