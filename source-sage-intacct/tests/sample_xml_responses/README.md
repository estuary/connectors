# Sample XML responses

Sage Intacct XML responses for the General Ledger objects, with the
structure of real responses and dummy values. Field names, data types and
Sage's own code words are kept. Record keys are renumbered, so joins between
the files still hold (`GLENTRYKEY` matches a GLENTRY `RECORDNO`).

- `json-*.xml.gz`: 100 rows of each object from a query that asked for JSON,
  rebuilt as XML.
- `lookup-*.xml`: the `lookup` response of each object.
- `sample-glbatch.xml.gz`, `full-glentry.xml`: rows with all fields.

Files larger than 100 KB are compressed with gzip.

Limits: timestamps and dates are not in the original order, equal values
share one dummy, and the counts in the `data` attributes are not real. The
connector asks Sage for JSON, and these files are XML. In Sage's JSON each
value is a string or null, and an empty element here stands for an empty
value: the tests read it as "" for BOOLEAN, ENUM and TEXT fields, and as a
missing value for the other types.
