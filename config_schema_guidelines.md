# JSON schema guidelines

These guidelines apply to JSON schemas that are associated with endpoint and resource configurations
for connectors. The purpose of this is to help us get consistent and robust UI forms, which are
generated from these schemas.

### Everything should have a `title` and a `description`

The `title` is rendered as the field label in the UI. Everything should have a sensible label.
The `description` is also rendered next to each field, and should generally be present for most
fields. Both `title` and `description` are pulled into the documentation, as well. 

Note that `description` must not contain any inline HTML, as it will not be rendered.

Additional things to keep in mind when writing descriptions:
* **Punctuation**: Descriptions should always end with a period, even when they're just sentence fragments.
* **Capitalization**: While _titles_ should be in Title Case, descriptions are capitalized like ordinary sentences.
* **Links**: Always use an [Estuary branded shortlink](https://app.short.io/public/login) for doc links in descriptions. 
Whenever possible, link to a relevant section in the Estuary docs rather than third-party docs.
* **Voice**: Use second person; refer to the reader as "you." Don't refer to "users," "developers," etc in descriptions.

### Group related fields within an object

Related fields should be grouped together in a separate JSON object, which has a `title` and a
`description`. This helps things render nicely so that the related fields are close together. For
example, an AWS access key and secret key should always be next to each other in the UI since they
always need to be provided together.

Example:

```json
{
    "type": "object",
    "title": "My Connector Config",
    "properties": {
        "awsCredentials": {
            "type": "object",
            "title": "AWS Credentials",
            "properties": {
                "accessKey": {
                    "type": "string",
                    "title": "AWS Access Key ID"
                },
                "secretKey": {
                    "type": "string",
                    "title": "AWS Secret Access Key"
                }
            }
        },
        "otherStuff": {...}
    }
}
```

### Always allow additional properties in endpoint configurations

The root schemas should always allow additional properties. This is the default, so you don't need
to add anything explicitly. Just _don't_ use `"additionalProperties": false`. This is because the
sops encryption will add a `sops` field at the root of the endpoint configuration, which will of
course cause validation to fail if your schema disallows it.

### Booleans should have a `default`

Boolean properties get rendered as a check box in the UI. Having a default
value helps to resolve the ambiguity when a user checks and then unchecks the box. Without the
`default`, the value will begin as `undefined`, and then after the user checks and unchecks the box,
it will be `false`. It's then ambiguous as to whether the connector will treat `undefined` and
`false` equivalently. Therefore, it seems best to always provide an explicit `default`, so that
boolean checkboxes never have `undefined` values.

Example:

```json
{
  "type": "boolean",
  "title": "Do the thing",
  "description": "whether or not to do the thing",
  "default": false
}
```

### Use `secret` annotation for all sensitive fields

Any kind of credentials or secrets need to be encrypted. We use the `secret` annotation to determine
which specific fields should be encrypted, and we leave the rest as plain text so that they can be
directly edited in the UI. Technically the `airbyte_secret` annotation will also work for this, but
we of course prefer plain `secret` where possible.

Example:

```json
{
    "title": "My connector config",
    "type": "object",
    "properties": {
        "apiKey": {
            "type": "string",
            "title": "Super Secret Software Stuff"
            "secret": true
        },
        "plainProperty": {
            "type": "string",
            "title": "Stuff that stays in plain text"
        }
    }
}
```

### Use `advanced` annotation for objects that should be collapsed by default

Some configuration tends to be used only in certain less common scenarios. Network tunneling is a
good example of this, as it's something that most users would want to ignore in most cases. Setting
`"advanced": true` in a schema with `"type": "object"` will cause the form for that object to be
collapsed by default. Users can still see the title, but they'll need to click on it to show all the
fields. This help to avoid overwhelming users with a bunch of fields that aren't relevant to what
they're doing.

Example:

```json
{
  "type": "object",
  "title": "My connector config",
  "properties": {
    "commonFields": {
      "title": "Common fields that most users need to consider",
      "type": "object",
      "properties": {
        "commonFieldA": {
          "type": "string",
          "title": "Some Common Config"
        },
        "commonFieldB": {
          "type": "integer",
          "title": "Common int field"
        }
      }
    },
    "uncommonFields": {
      "title": "Uncommon fields that might be overwhelming to users",
      "advanced": true,
      "type": "object",
      "properties": {
        "uncommonFieldA": {
          "type": "string",
          "title": "Some Uncommon Config"
        },
        "uncommonFieldB": {
          "type": "integer",
          "title": "Uncommon int field"
        }
      }
    }
  }
}
```

### Use `multiline` annotation for long strings

If a `string` field is allowed to contain newline characters, then you need to set
`"multiline": true` in order for the input to allow them. This will also cause the input to
dynamically expand as lines of text are added, so that the entire value can be shown.

Example:

```json
{
  "type": "string",
  "title": "Some long string value",
  "multiline": true
}
```

### Include `type` when using `enum`

Anytime you are using an enum in your schema you will need to include a type for it. This is for either simple cases (example 1) and for nested ones like example 2.

Example 1:

```json
{
  "type": "string",
  "enum": ["foo", "bar", "baz"],
  "title": "A single selection field"
}
```

Example 1:

```json
{
  "type": "array",
  "items": {
    "enum": ["foo", "bar", "buz"],
    "type": "string",
    "title": "Possible Choices",
    "description": "These are your choices"
  },
  "title": "Fields",
  "default": [],
  "description": "A list of selectable properties"
}
```
